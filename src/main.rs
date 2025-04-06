pub mod ino_cache;

use ino_cache::INOCache;
use noctfs::{
    self, BlockAddress, NoctFS,
    entity::{Entity, EntityFlags},
};

use std::{ffi::c_int, fs::File, io};

use fuse::{FileAttr, FileType, Filesystem, Request};
use libc::{EIO, ENOENT, ENOSYS, O_CREAT, O_RDONLY, O_WRONLY};

pub struct NoctFSFused<'a> {
    fs: NoctFS<'a>,
    global_fh: u64,
    fhs_opened: Vec<(u64, u64)>, // (fh, ino)
    ino_cache: INOCache,
}

pub mod device;

impl NoctFSFused<'_> {
    fn noct_search_by_block(&mut self, block: BlockAddress) -> Option<Entity> {
        let root = self.fs.get_root_entity();

        if let Err(e) = root {
            eprintln!("noct_search_by_block: {e}");
            return None;
        }

        let root = root.unwrap();

        if block == 1 {
            return Some(root);
        }

        let lsr = self.fs.list_directory(root.start_block);

        for i in &lsr {
            if i.is_directory() {
                return self.noct_search_by_block(i.start_block);
            }

            if i.start_block == block {
                return Some(i.clone());
            }
        }

        None
    }

    fn search_by_filename<T: ToString>(
        &mut self,
        directory_block: BlockAddress,
        name: T,
    ) -> Option<Entity> {
        let ents = self.fs.list_directory(directory_block);
        let name = name.to_string();

        for i in ents {
            if i.name == name {
                return Some(i.clone());
            }
        }

        None
    }

    fn next_fh(&mut self) -> u64 {
        let fh = self.global_fh;

        self.global_fh += 1;

        fh
    }

    fn allocate_fh(&mut self, fh: u64, ino: u64) {
        println!("Allocated fh: {fh} with ino: {ino}");
        self.fhs_opened.push((fh, ino));
    }

    fn is_fh_allocated(&self, fh: u64) -> bool {
        self.fhs_opened.iter().map(|x| x.0).any(|a| a == fh)
    }

    fn get_ino(&self, fh: u64) -> Option<u64> {
        if !self.is_fh_allocated(fh) {
            return None;
        }

        Some(
            self.fhs_opened
                .iter()
                .filter(|x| x.0 == fh)
                .next()
                .unwrap()
                .1,
        )
    }

    fn free_fh(&mut self, fh: u64) {
        println!("Freeing fh: {fh}");
        self.fhs_opened.retain(|a| a.0 != fh);
    }

    fn entity_attrs_to_fuse_attrs(&self, entity: &Entity) -> FileAttr {
        let no_ts = time::Timespec {
            sec: 0,
            nsec: 0,
        };

        FileAttr {
            ino: entity.start_block,
            size: entity.size,
            blocks: entity.size * self.fs.block_size() as u64,
            atime: time::get_time(),
            mtime: no_ts,
            ctime: no_ts,
            crtime: no_ts,
            kind: if entity.is_directory() {
                FileType::Directory
            } else {
                FileType::RegularFile
            },
            perm: 0o755,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
        }
    }
}

impl Filesystem for NoctFSFused<'_> {
    fn init(&mut self, _req: &fuse::Request) -> Result<(), c_int> {
        Ok(())
    }

    fn destroy(&mut self, _req: &fuse::Request) {}

    fn lookup(
        &mut self,
        _req: &fuse::Request,
        _parent: u64,
        _name: &std::ffi::OsStr,
        reply: fuse::ReplyEntry,
    ) {
        println!("lookup {_parent} {_name:?}");

        let entity = self.search_by_filename(_parent, _name.to_str().unwrap());

        if entity.is_none() {
            println!("lookup failed!");
            reply.error(ENOENT);
            return;
        }

        let entity = entity.unwrap();

        reply.entry(
            &time::get_time(),
            &self.entity_attrs_to_fuse_attrs(&entity),
            0,
        );

        self.ino_cache.add(_parent, entity.start_block);
    }

    fn forget(&mut self, _req: &fuse::Request, _ino: u64, _nlookup: u64) {}

    fn getattr(&mut self, _req: &fuse::Request, _ino: u64, reply: fuse::ReplyAttr) {
        println!("getattr {_ino}");

        if _ino == 1 {
            reply.attr(
                &time::get_time(),
                &FileAttr {
                    ino: _ino,
                    size: 4096,
                    blocks: 1,
                    atime: time::get_time(),
                    mtime: time::get_time(),
                    ctime: time::get_time(),
                    crtime: time::get_time(),
                    kind: FileType::Directory,
                    perm: 0o666,
                    nlink: 0,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    flags: 0,
                },
            );
        } else {
            let entity = self.noct_search_by_block(_ino);

            if entity.is_none() {
                reply.error(ENOENT);
                return;
            }

            let entity = entity.unwrap();
            reply.attr(
                &time::get_time(),
                &self.entity_attrs_to_fuse_attrs(&entity),
            );
        }
    }

    fn readlink(&mut self, _req: &fuse::Request, _ino: u64, reply: fuse::ReplyData) {
        println!("readlink");
        reply.error(ENOSYS);
    }

    fn mknod(
        &mut self,
        _req: &fuse::Request,
        _parent: u64,
        _name: &std::ffi::OsStr,
        _mode: u32,
        _rdev: u32,
        reply: fuse::ReplyEntry,
    ) {
        println!("u/i: mknod on {_parent} with name {_name:?}");

        reply.error(ENOSYS);
    }

    fn mkdir(
        &mut self,
        _req: &fuse::Request,
        _parent: u64,
        _name: &std::ffi::OsStr,
        _mode: u32,
        reply: fuse::ReplyEntry,
    ) {
        println!("mkdir on {_parent} with name {_name:?}");

        let entity = self.fs.create_directory(_parent, _name.to_str().unwrap());

        // reply.error(ENOSYS);
        
        reply.entry(
            &time::get_time(),
            &self.entity_attrs_to_fuse_attrs(&entity),
            0,
        );
        
        self.ino_cache.add(_parent, entity.start_block);
    }

    fn unlink(
        &mut self,
        _req: &fuse::Request,
        _parent: u64,
        _name: &std::ffi::OsStr,
        reply: fuse::ReplyEmpty,
    ) {
        println!("u/i: unlink on {_parent} with name {_name:?}");

        let entity = self.search_by_filename(_parent, _name.to_str().unwrap());

        if entity.is_none() {
            reply.error(ENOENT);
            return;
        }

        let entity = entity.unwrap();

        self.fs.delete_file(_parent, &entity);

        reply.ok();
        // reply.error(ENOSYS);
    }

    fn rmdir(
        &mut self,
        _req: &fuse::Request,
        _parent: u64,
        _name: &std::ffi::OsStr,
        reply: fuse::ReplyEmpty,
    ) {
        println!("u/i: rmdir on {_parent} with name {_name:?}");

        reply.error(ENOSYS);
    }

    fn symlink(
        &mut self,
        _req: &fuse::Request,
        _parent: u64,
        _name: &std::ffi::OsStr,
        _link: &std::path::Path,
        reply: fuse::ReplyEntry,
    ) {
        reply.error(ENOSYS);
    }

    fn rename(
        &mut self,
        _req: &fuse::Request,
        _parent: u64,
        _name: &std::ffi::OsStr,
        _newparent: u64,
        _newname: &std::ffi::OsStr,
        reply: fuse::ReplyEmpty,
    ) {
        println!(
            "u/i: renmae on {_parent} with name {_name:?}; new parent: {_newparent} with new name: {_newname:?}"
        );

        reply.error(ENOSYS);
    }

    fn link(
        &mut self,
        _req: &fuse::Request,
        _ino: u64,
        _newparent: u64,
        _newname: &std::ffi::OsStr,
        reply: fuse::ReplyEntry,
    ) {
        reply.error(ENOSYS);
    }

    fn open(&mut self, _req: &fuse::Request, _ino: u64, _flags: u32, reply: fuse::ReplyOpen) {
        println!("open {_ino}");

        let fh = self.next_fh();

        let read_flag = _flags & O_RDONLY as u32;
        let write_flag = _flags & O_WRONLY as u32;
        let create_flag = _flags & O_CREAT as u32;

        println!("Read: {read_flag}; Write: {write_flag}; Create: {create_flag}");

        reply.opened(fh, 0);
    }

    fn read(
        &mut self,
        _req: &fuse::Request,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _size: u32,
        reply: fuse::ReplyData,
    ) {
        println!("read ino/{_ino} fh/{_fh}");
        println!("ino from fh is: {:?}", self.get_ino(_fh));

        let dir_ino = self.ino_cache.find_parent(_ino);
        println!("ino cache returns: {:?}", dir_ino);

        if dir_ino.is_none() {
            println!("\x1b[31;1mNo parent directory! EIO!\x1b[0m");
            reply.error(EIO);
            return;
        }

        let dir_ino = dir_ino.unwrap();
        let ent = self.fs.get_entity_by_parent_and_block(dir_ino, _ino);

        if ent.is_none() {
            reply.error(ENOENT);
            return;
        }

        let ent = ent.unwrap();

        let mut data = vec![0u8; _size as usize];

        self.fs
            .read_contents_by_entity(&ent, &mut data, _offset as _)
            .unwrap();

        reply.data(data.as_slice());

        // reply.error(ENOSYS);
    }

    fn write(
        &mut self,
        _req: &fuse::Request,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _data: &[u8],
        _flags: u32,
        reply: fuse::ReplyWrite,
    ) {
        println!("\x1b[31mwrite\x1b[0m ino/{_ino}; fh/{_fh}");
        println!("ino from fh is: {:?}", self.get_ino(_fh));

        let dir_ino = self.ino_cache.find_parent(_ino);
        println!("ino cache returns: {:?}", dir_ino);

        if dir_ino.is_none() {
            println!("\x1b[31;1mNo parent directory! EIO!\x1b[0m");
            reply.error(EIO);
            return;
        }

        let dir_ino = dir_ino.unwrap();
        let ent = self.fs.get_entity_by_parent_and_block(dir_ino, _ino);

        if ent.is_none() {
            reply.error(ENOENT);
            return;
        }

        let ent = ent.unwrap();

        println!("Write on: {}", ent.name);

        self.fs
            .write_contents_by_entity(dir_ino, &ent, _data, _offset.try_into().unwrap());

        reply.written(_data.len() as _);
    }

    fn flush(
        &mut self,
        _req: &fuse::Request,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: fuse::ReplyEmpty,
    ) {
        // reply.error(ENOSYS);

        reply.ok();
    }

    fn release(
        &mut self,
        _req: &fuse::Request,
        _ino: u64,
        _fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
        reply: fuse::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsync(
        &mut self,
        _req: &fuse::Request,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: fuse::ReplyEmpty,
    ) {
        println!("fsync");
        // reply.error(ENOSYS);
        reply.ok();
    }

    fn opendir(&mut self, _req: &fuse::Request, _ino: u64, _flags: u32, reply: fuse::ReplyOpen) {
        println!("opendir {_ino} {_flags}");

        if _ino == 1 {
            let fh = self.next_fh();

            self.allocate_fh(fh, 1);

            reply.opened(fh, _flags);

            return;
        }

        println!("== Other dir!");
        let ent = self.noct_search_by_block(_ino);
        if ent.is_none() {
            reply.error(ENOENT);
            return;
        }
        let ent = ent.unwrap();

        if !ent.is_directory() {
            reply.error(ENOENT);
            return;
        }

        let fh = self.next_fh();
        self.allocate_fh(fh, _ino);

        reply.opened(fh, _flags);

        println!("Pushed fh: {}", fh);
    }

    fn readdir(
        &mut self,
        _req: &fuse::Request,
        mut _ino: u64,
        _fh: u64,
        _offset: i64,
        mut reply: fuse::ReplyDirectory,
    ) {
        println!("readdir {_ino} {_fh} {_offset}");

        if !self.is_fh_allocated(_fh) {
            reply.ok();
            return;
        }

        let ents = self.fs.list_directory(_ino);

        // println!("{ents:#?}");

        {
            // let parent_dir = self.ino_cache.find_parent(_ino);
            // reply.add()
        }

        for i in ents {
            reply.add(
                i.start_block,
                0,
                if i.is_directory() {
                    FileType::Directory
                } else {
                    FileType::RegularFile
                },
                i.name,
            );
        }
        reply.ok();

        self.free_fh(_fh);
    }

    fn releasedir(
        &mut self,
        _req: &fuse::Request,
        _ino: u64,
        _fh: u64,
        _flags: u32,
        reply: fuse::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsyncdir(
        &mut self,
        _req: &fuse::Request,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: fuse::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn statfs(&mut self, _req: &fuse::Request, _ino: u64, reply: fuse::ReplyStatfs) {
        reply.statfs(0, 0, 0, 0, 0, 512, 255, 0);
    }

    fn setxattr(
        &mut self,
        _req: &fuse::Request,
        _ino: u64,
        _name: &std::ffi::OsStr,
        _value: &[u8],
        _flags: u32,
        _position: u32,
        reply: fuse::ReplyEmpty,
    ) {
        println!("u/i: setxattr on {_ino} with name {_name:?}");
        reply.error(ENOSYS);
    }

    fn getxattr(
        &mut self,
        _req: &fuse::Request,
        _ino: u64,
        _name: &std::ffi::OsStr,
        _size: u32,
        reply: fuse::ReplyXattr,
    ) {
        println!("u/i: getxattr on {_ino} with name {_name:?}");
        reply.error(ENOSYS);
    }

    fn listxattr(&mut self, _req: &fuse::Request, _ino: u64, _size: u32, reply: fuse::ReplyXattr) {
        println!("u/i: listxattr on {_ino} with size: {_size}");
        reply.error(ENOSYS);
    }

    fn removexattr(
        &mut self,
        _req: &fuse::Request,
        _ino: u64,
        _name: &std::ffi::OsStr,
        reply: fuse::ReplyEmpty,
    ) {
        println!("u/i: removexattr on {_ino} with name {_name:?}");
        reply.error(ENOSYS);
    }

    fn access(&mut self, _req: &fuse::Request, _ino: u64, _mask: u32, reply: fuse::ReplyEmpty) {
        println!("access: {_ino} {_mask}");

        let a = self.noct_search_by_block(_ino);
        if a.is_none() {
            println!("access failed");
            reply.error(ENOENT);
            return;
        }

        println!("access succeeded");
        reply.ok();
    }

    fn create(
        &mut self,
        _req: &fuse::Request,
        mut _parent: u64,
        _name: &std::ffi::OsStr,
        _mode: u32,
        _flags: u32,
        reply: fuse::ReplyCreate,
    ) {
        println!(
            "Create {_name:?} on ino/{_parent} with mode(o) {_mode:o} and flags(x) {_flags:x}"
        );

        if _parent == 1 {
            _parent = self.fs.get_root_entity().unwrap().start_block;
        }

        let entry = self.fs.create_file(_parent, _name.to_str().unwrap());

        reply.created(
            &time::get_time(),
            &FileAttr {
                ino: entry.start_block,
                size: entry.size,
                blocks: entry.size * self.fs.block_size() as u64,
                atime: time::get_time(),
                mtime: time::get_time(),
                ctime: time::get_time(),
                crtime: time::get_time(),
                kind: if entry.flags.contains(EntityFlags::DIRECTORY) {
                    FileType::Directory
                } else {
                    FileType::RegularFile
                },
                perm: 0o666,
                nlink: 0,
                uid: 0,
                gid: 0,
                rdev: 0,
                flags: 0,
            },
            0,
            self.next_fh(),
            _flags,
        );

        self.ino_cache.add(_parent, entry.start_block);
    }

    fn getlk(
        &mut self,
        _req: &fuse::Request,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        _start: u64,
        _end: u64,
        _typ: u32,
        _pid: u32,
        reply: fuse::ReplyLock,
    ) {
        println!("u/i: getlk on {_ino} with fh {_fh}");

        reply.error(ENOSYS);
    }

    fn setlk(
        &mut self,
        _req: &fuse::Request,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        _start: u64,
        _end: u64,
        _typ: u32,
        _pid: u32,
        _sleep: bool,
        reply: fuse::ReplyEmpty,
    ) {
        println!("u/i: setlk on {_ino} with fh {_fh}");

        reply.error(ENOSYS);
    }

    fn bmap(
        &mut self,
        _req: &fuse::Request,
        _ino: u64,
        _blocksize: u32,
        _idx: u64,
        reply: fuse::ReplyBmap,
    ) {
        reply.error(ENOSYS);
    }

    fn setattr(
        &mut self,
        _req: &Request,
        _ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<time::Timespec>,
        _mtime: Option<time::Timespec>,
        _fh: Option<u64>,
        _crtime: Option<time::Timespec>,
        _chgtime: Option<time::Timespec>,
        _bkuptime: Option<time::Timespec>,
        _flags: Option<u32>,
        reply: fuse::ReplyAttr,
    ) {
        println!(
            "setattr on ino/{_ino}; mode: {_mode:?}, uid: {_uid:?}, gid: {_gid:?}, size: {_size:?}"
        );

        let entry = self.noct_search_by_block(_ino);

        if entry.is_none() {
            reply.error(ENOENT);
            return;
        }

        let entry = entry.unwrap();

        reply.attr(
            &time::get_time(),
            &FileAttr {
                ino: _ino,
                size: entry.size,
                blocks: entry.size * self.fs.block_size() as u64,
                atime: time::get_time(),
                mtime: time::get_time(),
                ctime: time::get_time(),
                crtime: time::get_time(),
                kind: if entry.is_directory() {
                    FileType::Directory
                } else {
                    FileType::RegularFile
                },
                perm: 0o666,
                nlink: 0,
                uid: 0,
                gid: 0,
                rdev: 0,
                flags: 0,
            },
        );
        // reply.error(ENOSYS);
    }
}

fn main() -> io::Result<()> {
    let filename = std::env::args().skip(1).last().expect("Specify a file!");

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(filename)
        .unwrap();
    let mut device = device::FileDevice(file);

    let fs = NoctFSFused {
        fhs_opened: vec![],
        fs: NoctFS::new(&mut device).unwrap(),
        global_fh: 0,
        ino_cache: INOCache::new(),
    };
    let mountpoint = String::from("../filesystem");

    std::fs::create_dir(&mountpoint)?;
    let result = fuse::mount(fs, &mountpoint, &[]);
    std::fs::remove_dir(mountpoint)?;

    println!("Result: {:?}", result);

    Ok(())
}
