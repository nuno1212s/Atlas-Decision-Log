use std::marker::PhantomData;
use std::sync::Arc;
use atlas_communication::reconfiguration_node::NetworkInformationProvider;
use atlas_core::ordering_protocol::loggable::{PersistentOrderProtocolTypes, PProof};
use atlas_core::ordering_protocol::networking::serialize::OrderingProtocolMessage;
use atlas_core::ordering_protocol::networking::signature_ver::OrderProtocolSignatureVerificationHelper;
use atlas_core::smr::networking::serialize::DecisionLogMessage;
use atlas_smr_application::serialize::ApplicationData;
use crate::decision_log::DecisionLog;

pub struct LogSerialization<D, OP, POP>(PhantomData<(D, OP, POP)>);

impl<D, OP, POP> DecisionLogMessage<D, OP, POP> for LogSerialization<D, OP, POP>
    where D: ApplicationData,
          OP: OrderingProtocolMessage<D>,
          POP: PersistentOrderProtocolTypes<D, OP> {
    type DecLogMetadata = ();
    type DecLog = DecisionLog<D, OP, POP>;
    type DecLogPart = Vec<PProof<D, OP, POP>>;

    fn verify_decision_log<NI, OPVH>(network_info: &Arc<NI>, dec_log: Self::DecLog) -> atlas_common::error::Result<Self::DecLog>
        where NI: NetworkInformationProvider,
              D: ApplicationData,
              OP: OrderingProtocolMessage<D>,
              POP: PersistentOrderProtocolTypes<D, OP>,
              OPVH: OrderProtocolSignatureVerificationHelper<D, OP, NI> {
        let mut proofs = Vec::with_capacity(dec_log.proofs().len());

        for proof in dec_log.into_proofs().into_iter() {
            let proof = POP::verify_proof::<NI, OPVH>(network_info, proof)?;

            proofs.push(proof);
        }

        Ok(DecisionLog::from_ordered_proofs(proofs))
    }
}