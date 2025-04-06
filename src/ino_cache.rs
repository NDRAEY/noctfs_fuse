pub struct CacheEntry {
    pub ino: u64,
    pub parent_ino: u64,
}

pub struct INOCache {
    container: Vec<CacheEntry>,
}

impl INOCache {
    pub fn new() -> Self {
        Self {
            container: Vec::with_capacity(256),
        }
    }

    pub fn add(&mut self, parent_ino: u64, ino: u64) {
        if self.container.len() > 256 {
            self.container.remove(0);
        }
        
        self.container.push(CacheEntry { ino, parent_ino });
    }

    pub fn find_parent(&mut self, ino: u64) -> Option<u64> {
        self.container
            .iter()
            .filter(|a| a.ino == ino)
            .next()
            .map(|a| a.parent_ino)
    }
}
