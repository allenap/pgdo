use super::{
    coordinate::{resource, CoordinateError, State},
    exists, Cluster, ClusterError,
};

pub type Resource<'a> = resource::ResourceFree<'a, Cluster>;

impl From<ClusterError> for CoordinateError<ClusterError> {
    fn from(err: ClusterError) -> Self {
        Self::ControlError(err)
    }
}

impl<'a> resource::Faceted<'a> for Cluster {
    type FacetFree = ClusterFree<'a>;
    type FacetShared = ClusterShared<'a>;
    type FacetExclusive = ClusterExclusive<'a>;

    fn facet_free(&'a self) -> Self::FacetFree {
        ClusterFree { cluster: self }
    }

    fn facet_shared(&'a self) -> Self::FacetShared {
        ClusterShared { cluster: self }
    }

    fn facet_exclusive(&'a self) -> Self::FacetExclusive {
        ClusterExclusive { cluster: self }
    }
}

pub struct ClusterFree<'a> {
    cluster: &'a Cluster,
}

impl<'a> ClusterFree<'a> {
    pub fn exists(&self) -> Result<bool, ClusterError> {
        Ok(exists(self.cluster))
    }
}

pub struct ClusterShared<'a> {
    cluster: &'a Cluster,
}

impl<'a> ClusterShared<'a> {
    pub fn exists(&self) -> Result<bool, ClusterError> {
        Ok(exists(self.cluster))
    }

    pub fn running(&self) -> Result<bool, ClusterError> {
        self.cluster.running()
    }
}

pub struct ClusterExclusive<'a> {
    cluster: &'a Cluster,
}

impl<'a> ClusterExclusive<'a> {
    pub fn start(&self) -> Result<State, ClusterError> {
        self.cluster.start()
    }

    pub fn stop(&self) -> Result<State, ClusterError> {
        self.cluster.stop()
    }

    pub fn destroy(&self) -> Result<State, ClusterError> {
        self.cluster.destroy()
    }

    pub fn exists(&self) -> Result<bool, ClusterError> {
        Ok(exists(self.cluster))
    }

    pub fn running(&self) -> Result<bool, ClusterError> {
        self.cluster.running()
    }
}
