use std::collections::VecDeque;
use either::Either;
use atlas_common::ordering::{InvalidSeqNo, SeqNo};
use atlas_core::ordering_protocol::DecisionMetadata;
use atlas_core::smr::smr_decision_log::StoredConsensusMessage;

/// A struct to store the ongoing decision known parameters
pub struct OnGoingDecision<D, OP, POP> {
    // The seq number of this decision
    seq: SeqNo,

    completed: bool,

    // The metadata of the decision, optional since it's usually the
    metadata: Option<DecisionMetadata<D, OP>>,

    // The messages that compose this decision, to be transformed into a given proof
    messages: Vec<StoredConsensusMessage<D, OP, POP>>,
}

/// The log for decisions which are currently being decided
pub struct DecidingLog<D, OP, POP> {
    // The seq no of the first decision in the queue
    curr_seq: SeqNo,

    // The currently deciding list. This is a vec deque since we can only decide seqno n when
    // all seqno < n have already been decided
    currently_deciding: VecDeque<OnGoingDecision<D, OP, POP>>,
}

impl<D, OP, POP> DecidingLog<D, OP, POP> {
    pub fn init(default_capacity: usize, starting_seq: SeqNo) -> Self {
        Self {
            curr_seq: starting_seq,
            currently_deciding: VecDeque::with_capacity(default_capacity),
        }
    }

    pub fn clear_decision_at(&mut self, seq: SeqNo) {
        todo!()
    }

    pub fn clear_seq_forward_of(&mut self, seq: SeqNo) {
        let index = seq.index(self.curr_seq);

        match index {
            Either::Right(index) => {
                for i in index..self.currently_deciding.len() {}
            }
            Either::Left(_) => {
                unreachable!("Progressed decision that has already been decided?")
            }
        }
    }

    fn decision_at_index(&mut self, index: usize) -> &mut OnGoingDecision<D, OP, POP> {

        if self.currently_deciding.len() > index {
            self.currently_deciding.get_mut(index).unwrap()
        } else {
             let to_create = (self.currently_deciding.len() - index) + 1;

            let mut start_seq = self.currently_deciding.back()
                .map(|decision| decision.seq_no().next())
                .unwrap_or(self.curr_seq);

            for _ in 0..to_create {
                self.currently_deciding.push_back(OnGoingDecision::init(start_seq));

                start_seq = start_seq.next();
            }

            self.currently_deciding.get_mut(index).unwrap()
        }
    }

    pub fn decision_progressed(&mut self, seq: SeqNo, message: StoredConsensusMessage<D, OP, POP>) {
        let index = seq.index(self.curr_seq);

        match index {
            Either::Right(index) => {
                self.decision_at_index(index).insert_component_message(message);
            }
            Either::Left(_) => {
                unreachable!("Progressed decision that has already been decided?")
            }
        }
    }

    pub fn complete_decision(&mut self, seq: SeqNo, metadata: DecisionMetadata<D, OP>) {
        let index = seq.index(self.curr_seq);

        match index {
            Either::Right(index) => {
                let decision = self.decision_at_index(index);

                decision.insert_metadata(metadata);

                decision.completed();
            }
            Either::Left(_) => {
                unreachable!("Completing decision that has already been decided")
            }
        }
    }

    /// Get the pending decisions that already have all of the necessary information
    /// to be completed
    pub fn complete_pending_decisions(&mut self) -> Vec<OnGoingDecision<D, OP, POP>> {
        let mut decisions = vec![];

        while !self.currently_deciding.is_empty() {
            if self.currently_deciding.front().unwrap().is_completed() {
                let decision = self.currently_deciding.pop_front().unwrap();

                decisions.push(decision);

                self.curr_seq = self.curr_seq.next();
            }
        }

        decisions
    }
}

impl<D, OP, POP> OnGoingDecision<D, OP, POP> {
    fn init(seq: SeqNo) -> Self {
        Self {
            seq,
            completed: false,
            metadata: None,
            messages: vec![],
        }
    }

    fn seq_no(&self )-> SeqNo {
        self.seq
    }

    fn insert_metadata(&mut self, metadata: DecisionMetadata<D, OP>) {
        let _ = self.metadata.insert(metadata);
    }

    fn insert_component_message(&mut self, partial: StoredConsensusMessage<D, OP, POP>) {
        self.messages.push(partial)
    }

    fn completed(&mut self) {
        self.completed = true;
    }

    fn is_completed(&self) -> bool {
        self.completed
    }

    pub fn into_components(self) -> (DecisionMetadata<D, OP>, Vec<StoredConsensusMessage<D, OP, POP>>) {
        (self.metadata, self.messages)
    }
}