use super::*;

impl BlockBuilder {
    pub(crate) fn peer_list(&self) -> &PeerList {
        &self.peer_list
    }

    pub(crate) fn acquisition_state(&self) -> &BlockAcquisitionState {
        &self.acquisition_state
    }
}
