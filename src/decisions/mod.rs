use atlas_common::ordering::{Orderable, SeqNo};
use atlas_core::ordering_protocol::DecisionMetadata;
use atlas_core::smr::smr_decision_log::StoredConsensusMessage;
use atlas_smr_application::app::UpdateBatch;
use atlas_smr_application::serialize::ApplicationData;

/// A struct to store the ongoing decision known parameters
pub struct OnGoingDecision<D, OP, POP> where D: ApplicationData {
    // The seq number of this decision
    seq: SeqNo,
    // Whether this decision has been marked as completed by the ordering protocol
    completed: bool,
    // The metadata of the decision, optional since it's usually the
    metadata: Option<DecisionMetadata<D, OP>>,
    // The messages that compose this decision, to be transformed into a given proof
    messages: Vec<StoredConsensusMessage<D, OP, POP>>,
    // The batch of client requests, unwrapped from the ordering protocol as we want
    // To maintain the consensus message as a black box
    update_batch: Option<UpdateBatch<D::Request>>,
}

/// The completed decision object with all necessary information to be transformed
/// into a proof, which will be put into the decision log
pub struct CompletedDecision<D, OP, POP> where D: ApplicationData {
    seq: SeqNo,
    metadata: DecisionMetadata<D, OP>,
    messages: Vec<StoredConsensusMessage<D, OP, POP>>,
    client_rq_batch: UpdateBatch<D::Request>,
}

impl<D, OP, POP> Orderable for OnGoingDecision<D, OP, POP> where D: ApplicationData {
    fn sequence_number(&self) -> SeqNo {
        self.seq
    }
}

impl<D, OP, POP> OnGoingDecision<D, OP, POP> where D: ApplicationData {
    pub fn init(seq: SeqNo) -> Self {
        Self {
            seq,
            completed: false,
            metadata: None,
            update_batch: None,
            messages: vec![],
        }
    }

    pub fn insert_metadata(&mut self, metadata: DecisionMetadata<D, OP>) {
        let _ = self.metadata.insert(metadata);
    }

    pub fn insert_component_message(&mut self, partial: StoredConsensusMessage<D, OP, POP>) {
        self.messages.push(partial)
    }

    pub fn insert_requests(&mut self, requests: UpdateBatch<D::Request>) {
        self.update_batch = Some(requests);
    }

    pub fn completed(&mut self) {
        self.completed = true;
    }

    pub  fn is_completed(&self) -> bool {
        self.completed
    }

    pub fn into_components(self) -> (DecisionMetadata<D, OP>, Vec<StoredConsensusMessage<D, OP, POP>>) {
        (self.metadata, self.messages)
    }
}


