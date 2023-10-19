use crate::lock;

use super::{
    coordinate::{resource, State},
    exists, Cluster, ClusterError,
};

pub type Resource<'a> = resource::ResourceFree<'a, Cluster>;

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

pub fn start_exclusive(cluster: Cluster) -> Result<(), ClusterError> {
    let lock = lock::UnlockedFile::try_from(&cluster.datadir)?;
    let resource = resource::ResourceFree::new(lock, cluster);
    let resource = resource.exclusive().unwrap();
    let facet = resource.facet();
    facet.start()?;
    let resource = resource.shared().unwrap();
    let facet = resource.facet();
    println!("running: {}", facet.running()?);
    let resource = resource.exclusive().unwrap();
    resource.facet().stop()?;
    resource.release().unwrap();
    Ok(())
}
