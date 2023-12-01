use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::vec;

use futures::channel::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures::channel::oneshot;
use futures::executor::ThreadPool;
use futures::select;
use futures::task::SpawnExt;
use futures::FutureExt;
use futures::StreamExt;
use rand::Rng;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
pub enum ApplyMsg {
    Command {
        data: Vec<u8>,
        index: u64,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: u64,
        index: u64,
    },
}

enum Event {
    ResetTimeout,
    Timeout,
    HeartBeat,
    RequestVoteReply(usize, RequestVoteReply),
    AppendEntriesReply(usize, usize, AppendEntriesReply),
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

enum RoleState {
    Leader {
        next_index: Vec<usize>,
        match_index: Vec<usize>,
    },
    Candidate {
        get_vote: u64,
    },
    Follower,
}

// #[derive(Debug)]
// struct PersistentState {
//     current_term: u64,
//     voted_for: Option<u64>,
//     log: Vec<Entry>,
// }

impl PersistentState {
    pub fn new() -> Self {
        PersistentState {
            current_term: 0,
            voted_for: None,
            log: vec![Default::default()],
        }
    }
}

#[derive(Default)]
struct VolatileState {
    commit_index: usize,
    last_applied: usize,
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

    // Role state
    role_state: RoleState,
    // Persistent state
    persistent_state: PersistentState,
    // Volatile state
    volatile_state: VolatileState,
    // Channel
    apply_ch: UnboundedSender<ApplyMsg>,
    event_loop_tx: Option<UnboundedSender<Event>>,
    // Executor
    executor: ThreadPool,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            role_state: RoleState::Follower,
            persistent_state: PersistentState::new(),
            volatile_state: VolatileState::default(),
            apply_ch,
            event_loop_tx: None,
            executor: ThreadPool::new().unwrap(),
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);
        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
        let mut data = Vec::new();
        labcodec::encode(&self.persistent_state, &mut data).unwrap();
        self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
        match labcodec::decode(data) {
            Ok(state) => {
                self.persistent_state = state;
            }
            Err(e) => panic!("{:?}", e),
        }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(
        &self,
        server: usize,
        args: RequestVoteArgs,
    ) -> Receiver<Result<RequestVoteReply>> {
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let peer = &self.peers[server];
        // let peer_clone = peer.clone();
        // let (tx, rx) = channel();
        // peer.spawn(async move {
        //     let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
        //     tx.send(res);
        // });
        // rx
        // ```
        let (tx, rx) = sync_channel::<Result<RequestVoteReply>>(1);
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        peer.spawn(async move {
            let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
            tx.send(res).expect("Can't send");
        });
        rx
    }

    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        if !matches!(self.role_state, RoleState::Leader { .. }) {
            // info!("Peer: {}, NotLeader", self.me);
            return Err(Error::NotLeader);
        }

        let index = self.persistent_state.log.len() as u64;
        let term = self.persistent_state.current_term;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).
        // info!(
        //     "Peer: {}, Recive command: {:?}, index: {}",
        //     self.me, &buf, index
        // );
        self.persistent_state.log.push(Entry { term, data: buf });

        for (peer, clinet) in self.peers.iter().enumerate() {
            if peer == self.me {
                continue;
            }

            let args = self.append_entries_args(peer);
            let fut = clinet.append_entries(&args);
            let tx = self.event_loop_tx.clone().unwrap();
            let log_len = self.persistent_state.log.len();

            self.executor
                .spawn(async move {
                    if let Ok(append_entries_reply) = fut.await {
                        tx.unbounded_send(Event::AppendEntriesReply(
                            log_len,
                            peer,
                            append_entries_reply,
                        ))
                        .expect("Can't send");
                    }
                })
                .expect("Can't spawn");
        }

        Ok((index, term))
    }

    fn cond_install_snapshot(
        &mut self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here (2D).
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    fn snapshot(&mut self, index: u64, snapshot: &[u8]) {
        // Your code here (2D).
        crate::your_code_here((index, snapshot));
    }
}

// translate status
impl Raft {
    fn trans_to_follower(&mut self) {
        self.role_state = RoleState::Follower;
    }

    fn trans_to_candidate(&mut self) {
        self.role_state = RoleState::Candidate { get_vote: 1 };
        self.persistent_state.voted_for = Some(self.me as u64);
    }

    fn trans_to_leader(&mut self) {
        // println!("{}: Trans to Leader", self.me);
        self.role_state = RoleState::Leader {
            next_index: vec![self.persistent_state.log.len(); self.peers.len()],
            match_index: vec![0; self.peers.len()],
        };
        self.send_heart_beat();
    }

    fn trans_term(&mut self, term: u64) {
        self.persistent_state.current_term = term;
        self.persistent_state.voted_for = None;
    }
}

// args
impl Raft {
    fn append_entries_args(&self, peer: usize) -> AppendEntriesArgs {
        // prev_log_index
        let index = {
            match &self.role_state {
                RoleState::Leader { next_index, .. } => next_index[peer] - 1,
                _ => unreachable!(),
            }
        };

        // prev_log_term
        let prev_log_term = self.persistent_state.log[index].term;

        // entries
        let entries = if index + 1 >= self.persistent_state.log.len() {
            Vec::new()
        } else {
            self.persistent_state.log[(index + 1)..]
                .iter()
                .cloned()
                .collect()
        };

        AppendEntriesArgs {
            term: self.persistent_state.current_term,
            leader_id: self.me as u64,
            prev_log_index: index as u64,
            prev_log_term,
            entries,
            leader_commit: self.volatile_state.commit_index as u64,
        }
    }

    fn request_vote_args(&self) -> RequestVoteArgs {
        RequestVoteArgs {
            term: self.persistent_state.current_term,
            candidate_id: self.me as u64,
            last_log_index: self.persistent_state.log.len() as u64,
            last_log_term: self.persistent_state.log.last().unwrap().term,
        }
    }
}

// handle events
impl Raft {
    fn handle_event(&mut self, event: Event) {
        match event {
            Event::ResetTimeout => unreachable!(),
            Event::Timeout => self.handle_timeout(),
            Event::HeartBeat => self.send_heart_beat(),
            Event::RequestVoteReply(from, request_vote_reply) => {
                self.persist();
                self.handle_request_vote_reply(from, request_vote_reply)
            }
            Event::AppendEntriesReply(log_len, from, append_entries_reply) => {
                self.persist();
                self.handle_append_entries_reply(log_len, from, append_entries_reply)
            }
        }
    }

    // Candidate and Follower: begin election
    fn handle_timeout(&mut self) {
        match self.role_state {
            RoleState::Candidate { .. } | RoleState::Follower => {
                self.trans_term(self.persistent_state.current_term + 1);
                self.trans_to_candidate();

                // // println!("CANDIDATE: PEERS {}", self.peers.len());
                for (peer, client) in self.peers.iter().enumerate() {
                    if peer == self.me {
                        continue;
                    }

                    let args = self.request_vote_args();
                    let fut = client.request_vote(&args);
                    let tx = self.event_loop_tx.clone().unwrap();

                    self.executor
                        .spawn(async move {
                            if let Ok(append_entries_reply) = fut.await {
                                tx.unbounded_send(Event::RequestVoteReply(
                                    peer,
                                    append_entries_reply,
                                ))
                                .expect("Can't send");
                            }
                        })
                        .expect("Can't spawn");
                }
            }
            _ => {}
        }
    }

    // Leader: send heart beat
    fn send_heart_beat(&self) {
        match &self.role_state {
            RoleState::Leader { .. } => {
                // println!("{}: Send Heart Beat", self.me);
                for (peer, clinet) in self.peers.iter().enumerate() {
                    if peer == self.me {
                        continue;
                    }

                    let args = self.append_entries_args(peer);
                    let fut = clinet.append_entries(&args);
                    let tx = self.event_loop_tx.clone().unwrap();
                    let log_len = self.persistent_state.log.len();

                    self.executor
                        .spawn(async move {
                            if let Ok(append_entries_reply) = fut.await {
                                tx.unbounded_send(Event::AppendEntriesReply(
                                    log_len,
                                    peer,
                                    append_entries_reply,
                                ))
                                .expect("Can't send");
                            }
                        })
                        .expect("Can't spawn");
                }
            }
            _ => {}
        }
    }

    // All: vote
    fn handle_request_vote(&mut self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        let RequestVoteArgs {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        } = args;

        if term > self.persistent_state.current_term {
            self.trans_term(term);
            self.trans_to_follower();
        }

        let vote_granted = {
            // term
            if term < self.persistent_state.current_term {
                false
            }
            // voted for
            else if self.persistent_state.voted_for.is_some()
                && self.persistent_state.voted_for.unwrap() as u64 != candidate_id
            {
                false
            }
            // log is up-to-date(section 5.4.1)
            else if last_log_term < self.persistent_state.log.last().unwrap().term
                || (last_log_term == self.persistent_state.log.last().unwrap().term
                    && last_log_index < self.persistent_state.log.len() as u64)
            {
                false
            } else {
                self.persistent_state.voted_for = Some(candidate_id);
                true
            }
        };

        Ok(RequestVoteReply {
            term: self.persistent_state.current_term,
            vote_granted,
        })
    }

    // Candidate: handle vote reply
    fn handle_request_vote_reply(&mut self, _from: usize, reply: RequestVoteReply) {
        let RequestVoteReply { term, vote_granted } = reply;

        if term > self.persistent_state.current_term {
            self.trans_term(term);
            self.trans_to_follower();
            return;
        }

        match &mut self.role_state {
            RoleState::Candidate { get_vote } => {
                if term > self.persistent_state.current_term {
                    self.trans_term(term);
                    self.trans_to_follower();
                    return;
                }

                if vote_granted {
                    *get_vote += 1;
                    if *get_vote >= (self.peers.len() + 1) as u64 / 2 {
                        self.trans_to_leader();
                    }
                }
            }
            _ => {}
        }
    }

    // Candidate: trans to Follower
    // Follower: update log
    fn handle_append_entries(
        &mut self,
        args: AppendEntriesArgs,
    ) -> labrpc::Result<AppendEntriesReply> {
        // println!("{}: Receive Append Entries", self.me);

        let AppendEntriesArgs {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        } = args;
        let prev_log_index = prev_log_index as usize;
        let leader_commit = leader_commit as usize;
        // println!("{}: receive entry {:?}", self.me, entries);

        if term > self.persistent_state.current_term {
            self.trans_term(term);
            self.trans_to_follower();
        }

        if term == self.persistent_state.current_term {
            if matches!(self.role_state, RoleState::Candidate { .. }) {
                self.trans_to_follower();
            }
            self.event_loop_tx
                .clone()
                .unwrap()
                .unbounded_send(Event::ResetTimeout)
                .expect("Can't send");
        }

        let success = {
            if term < self.persistent_state.current_term {
                false
            } else if self.persistent_state.log.len() <= prev_log_index
                || self.persistent_state.log[prev_log_index].term != prev_log_term
            {
                false
            } else {
                true
            }
        };

        if success {
            let sync_index = prev_log_index + entries.len();

            // update log
            for (index, entry) in entries.into_iter().enumerate() {
                let index = prev_log_index + index + 1;

                if index >= self.persistent_state.log.len() {
                    self.persistent_state.log.push(entry);
                } else if self.persistent_state.log[index].term != entry.term {
                    self.persistent_state.log.truncate(index);
                    self.persistent_state.log.push(entry);
                }
            }

            // commit index
            if leader_commit > self.volatile_state.commit_index {
                self.volatile_state.commit_index = leader_commit.min(sync_index);
                self.update_apply();
            }

            // TODO: redirect(but how?)
            let _ = leader_id;
        }

        Ok(AppendEntriesReply {
            term: self.persistent_state.current_term,
            success,
        })
    }

    // Leader: handle append entries reply
    fn handle_append_entries_reply(
        &mut self,
        log_len: usize,
        from: usize,
        append_entries_reply: AppendEntriesReply,
    ) {
        let AppendEntriesReply { term, success } = append_entries_reply;

        if term > self.persistent_state.current_term {
            self.trans_to_follower();
            return;
        }

        // change next and match index
        match &mut self.role_state {
            RoleState::Leader {
                next_index,
                match_index,
            } => {
                if next_index[from] < self.persistent_state.log.len() {
                    if success {
                        next_index[from] = next_index[from].max(log_len);
                        match_index[from] = match_index[from].max(log_len - 1);
                    } else {
                        // info!(
                        //     "Peer {} updata log fail on index {}",
                        //     from, next_index[from]
                        // );
                        next_index[from] -= 1;
                        let args = self.append_entries_args(from);
                        self.peers[from].append_entries(&args);
                    }
                }
            }
            _ => {}
        }

        // commit
        match &self.role_state {
            RoleState::Leader { match_index, .. } => {
                let success_index = match_index[from];
                if success
                    && term == self.persistent_state.current_term
                    && success_index > self.volatile_state.commit_index
                {
                    let mut sum = 1;
                    for i in 0..self.peers.len() {
                        if i == self.me {
                            continue;
                        }
                        if match_index[i] >= success_index {
                            sum += 1;
                        }
                    }

                    if sum >= (self.peers.len() + 1) / 2 {
                        self.volatile_state.commit_index =
                            self.volatile_state.commit_index.max(success_index);
                        self.update_apply();
                        // self.send_heart_beat();
                    }
                }
            }
            _ => {}
        }
    }
}

impl Raft {
    fn update_apply(&mut self) {
        let begin = self.volatile_state.last_applied + 1;
        let end = self.volatile_state.commit_index;

        for index in begin..=end {
            // info!("{}: Commit {}", self.me, index);
            self.apply_ch
                .unbounded_send(ApplyMsg::Command {
                    data: self.persistent_state.log[index].data.clone(),
                    index: index as u64,
                })
                .expect("Can't send");
        }

        self.volatile_state.last_applied = end;
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        // let _ = self.start(&0);
        let _ = self.cond_install_snapshot(0, 0, &[]);
        self.snapshot(0, &[]);
        let _ = self.send_request_vote(0, Default::default());
        self.persist();
        // let _ = &self.state;
        // let _ = &self.apply_ch;
        // let _ = &self.me;
        let _ = &self.persister;
        // let _ = &self.peers;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    raft: Arc<Mutex<Raft>>,
    event_loop_tx: UnboundedSender<Event>,
    executor: ThreadPool,
    shutdown_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(mut raft: Raft) -> Node {
        // Your code here.
        let (event_loop_tx, event_loop_rx) = mpsc::unbounded();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        raft.event_loop_tx = Some(event_loop_tx.clone());

        let node = Node {
            raft: Arc::new(Mutex::new(raft)),
            event_loop_tx,
            executor: ThreadPool::new().unwrap(),
            shutdown_tx: Arc::new(Mutex::new(Some(shutdown_tx))),
        };
        node.start_event_loop(event_loop_rx, shutdown_rx);
        node
    }

    fn start_event_loop(
        &self,
        mut event_loop_rx: UnboundedReceiver<Event>,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) {
        let raft = Arc::clone(&self.raft);
        let event_loop_tx = self.event_loop_tx.clone();

        self.executor
            .spawn(async move {
                let election_builder = || {
                    futures_timer::Delay::new(Duration::from_millis(
                        rand::thread_rng().gen_range(150, 300),
                    ))
                    .fuse()
                };
                let heart_beat_builder =
                    || futures_timer::Delay::new(Duration::from_millis(50)).fuse();

                let mut election_timeout = election_builder();
                let mut heart_beat_timeout = heart_beat_builder();

                loop {
                    select! {
                        event = event_loop_rx.select_next_some() => {
                            match event {
                                Event::ResetTimeout => election_timeout = election_builder(),
                                event => raft.lock().unwrap().handle_event(event),
                            }
                        }
                        _ = election_timeout => {
                            event_loop_tx.unbounded_send(Event::Timeout).unwrap();
                            election_timeout = election_builder();
                        },
                        _ = heart_beat_timeout => {
                            event_loop_tx.unbounded_send(Event::HeartBeat).unwrap();
                            heart_beat_timeout = heart_beat_builder();
                        }
                        _ = shutdown_rx => break,
                    }
                }
            })
            .expect("can't spawn loop");
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        // crate::your_code_here(())
        self.raft.lock().unwrap().start(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        // crate::your_code_here(())
        self.raft.lock().unwrap().persistent_state.current_term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        // crate::your_code_here(())
        matches!(
            self.raft.lock().unwrap().role_state,
            RoleState::Leader { .. }
        )
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
        if let Some(sender) = self.shutdown_tx.lock().unwrap().take() {
            sender.send(()).unwrap();
        }
    }

    /// A service wants to switch to snapshot.  
    ///
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub fn cond_install_snapshot(
        &self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here.
        // Example:
        // self.raft.cond_install_snapshot(last_included_term, last_included_index, snapshot)
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub fn snapshot(&self, index: u64, snapshot: &[u8]) {
        // Your code here.
        // Example:
        // self.raft.snapshot(index, snapshot)
        crate::your_code_here((index, snapshot));
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        // Your code here (2A, 2B).
        let raft = self.raft.clone();
        self.executor
            .spawn_with_handle(async move {
                // debug!("vote: try lock");
                let mut raft = raft.lock().unwrap();
                raft.persist();
                let res = raft.handle_request_vote(args);
                // debug!("vote: release lock");
                res
            })
            .unwrap()
            .await
    }
    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        let raft = self.raft.clone();
        self.executor
            .spawn_with_handle(async move {
                // debug!("append entries: try lock");
                let mut raft = raft.lock().unwrap();
                raft.persist();
                let res = raft.handle_append_entries(args);
                // debug!("append entries: release lock");
                res
            })
            .unwrap()
            .await
    }
}
