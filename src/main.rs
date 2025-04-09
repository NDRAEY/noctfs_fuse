pub mod ino_cache;

use ino_cache::INOCache;
use noctfs::{self, BlockAddress, NoctFS, entity::Entity};

use std::{
    ffi::OsStr,
    io,
    time::{Duration, SystemTime},
};

use fuser::{FileAttr, FileType, Filesystem, MountOption, Request};
use libc::{EIO, ENOENT, ENOSYS, O_ACCMODE, O_RDONLY, O_RDWR, O_WRONLY};

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
            if [".", ".."].contains(&i.name.as_str()) {
                continue;
            }

            if i.start_block == block {
                return Some(i.clone());
            }
 
            if i.is_directory() {
                return self.noct_search_by_block(i.start_block);
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

        Some(self.fhs_opened.iter().find(|x| x.0 == fh).unwrap().1)
    }

    fn free_fh(&mut self, fh: u64) {
        println!("Freeing fh: {fh}");
        self.fhs_opened.retain(|a| a.0 != fh);
    }

    fn entity_attrs_to_fuse_attrs(&self, entity: &Entity) -> FileAttr {
        let no_ts = SystemTime::UNIX_EPOCH;

        FileAttr {
            ino: entity.start_block,
            size: entity.size,
            blocks: entity.size * self.fs.block_size() as u64,
            atime: SystemTime::now(),
            mtime: no_ts,
            ctime: no_ts,
            crtime: no_ts,
            kind: if entity.is_directory() {
                FileType::Directory
            } else {
                FileType::RegularFile
            },
            perm: 0o644,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
            blksize: self.fs.block_size() as u32,
        }
    }
}

const DEFAULT_DURATION: Duration = Duration::from_secs(3600);

impl Filesystem for NoctFSFused<'_> {
    fn init(
        &mut self,
        _req: &Request<'_>,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        Ok(())
    }

    fn destroy(&mut self) {}

    fn lookup(
        &mut self,
        _req: &fuser::Request,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        println!("lookup(parent: {:#x?}, name {:?})", parent, name);

        let entity = self.search_by_filename(parent, name.to_str().unwrap());

        if entity.is_none() {
            println!("lookup failed!");
            reply.error(ENOENT);
            return;
        }

        let entity = entity.unwrap();
        self.ino_cache.add(parent, entity.start_block);

        println!("{name:?} is ino {}", entity.start_block);

        reply.entry(
            &DEFAULT_DURATION,
            &self.entity_attrs_to_fuse_attrs(&entity),
            0,
        );
    }

    fn forget(&mut self, _req: &fuser::Request, _ino: u64, _nlookup: u64) {}

    fn getattr(
        &mut self,
        _req: &fuser::Request,
        ino: u64,
        _fh: Option<u64>,
        reply: fuser::ReplyAttr,
    ) {
        println!("getattr on ino/{ino}");

        if ino == 1 {
            reply.attr(
                &DEFAULT_DURATION,
                &FileAttr {
                    ino: ino,
                    size: 4096,
                    blocks: 1,
                    atime: SystemTime::now(),
                    mtime: SystemTime::UNIX_EPOCH,
                    ctime: SystemTime::UNIX_EPOCH,
                    crtime: SystemTime::UNIX_EPOCH,
                    kind: FileType::Directory,
                    perm: 0o644,
                    nlink: 0,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    flags: 0,
                    blksize: self.fs.block_size() as u32,
                },
            );
        } else {
            let entity = self.noct_search_by_block(ino);

            if entity.is_none() {
                println!("\x1b[31;1mNo entry! ENOENT!\x1b[0m");

                reply.error(ENOENT);
                return;
            }

            let entity = entity.unwrap();
            reply.attr(&DEFAULT_DURATION, &self.entity_attrs_to_fuse_attrs(&entity));
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: fuser::ReplyAttr,
    ) {
        println!(
            "setattr(ino: {:#x?}, mode: {:?}, uid: {:?}, \
            gid: {:?}, size: {:?}, fh: {:?}, flags: {:?})",
            ino, mode, uid, gid, size, fh, flags
        );



        let entity = self.noct_search_by_block(ino);

        if entity.is_none() {
            reply.error(ENOENT);
            return;
        }

        let entity = entity.unwrap();

        println!("Found entity: {entity:?}");

        let mut new_entity = entity.clone();

        if let Some(size) = size {
            println!("Want to trunc to: {}!", size);

            if size > entity.size  {
                println!("TODO! TODO! TODO! Make file bigger! Current size is: {}, setattr wants: {size}", entity.size);
            }

            new_entity.size = size;

            let parent = self.ino_cache.find_parent(ino);

            if let Some(directory_block) = parent {
                println!("Writing meta");

                match self.fs.overwrite_entity_header(directory_block, &entity, &new_entity) {
                    Some(()) => println!("Success!"),
                    None => println!("Fail!"),
                }
            } else {
                println!("[Error] No parent!");
            }
        }

        reply.attr(&DEFAULT_DURATION, &self.entity_attrs_to_fuse_attrs(&new_entity));
    }

    fn readlink(&mut self, _req: &fuser::Request, _ino: u64, reply: fuser::ReplyData) {
        println!("u/i: readlink on ino/{_ino}");

        reply.error(ENOSYS);
    }

    fn mknod(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: fuser::ReplyEntry,
    ) {
        println!("u/i: mknod on {parent} with name {name:?}");

        reply.error(ENOSYS);
    }

    fn mkdir(
        &mut self,
        _req: &fuser::Request,
        parent: u64,
        _name: &std::ffi::OsStr,
        _mode: u32,
        _umask: u32,
        reply: fuser::ReplyEntry,
    ) {
        println!("mkdir on {parent} with name {_name:?}");

        let entity = self.fs.create_directory(parent, _name.to_str().unwrap());

        reply.entry(
            &DEFAULT_DURATION,
            &self.entity_attrs_to_fuse_attrs(&entity),
            0,
        );

        self.ino_cache.add(parent, entity.start_block);
    }

    fn unlink(
        &mut self,
        _req: &fuser::Request,
        _parent: u64,
        _name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        println!("u/i: unlink on {_parent} with name {_name:?}");

        let entity = self.search_by_filename(_parent, _name.to_str().unwrap());

        if entity.is_none() {
            println!("\x1b[31;1mNo entry! ENOENT!\x1b[0m");
            reply.error(ENOENT);
            return;
        }

        let entity = entity.unwrap();

        self.fs.delete_file(_parent, &entity);

        reply.ok();
    }

    fn rmdir(
        &mut self,
        _req: &fuser::Request,
        _parent: u64,
        _name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        println!("u/i: rmdir on {_parent} with name {_name:?}");

        reply.error(ENOSYS);
    }

    fn symlink(
        &mut self,
        _req: &fuser::Request,
        _parent: u64,
        _name: &std::ffi::OsStr,
        _link: &std::path::Path,
        reply: fuser::ReplyEntry,
    ) {
        println!("u/i: symlink on {_parent}, name: {_name:?}");

        reply.error(ENOSYS);
    }

    fn rename(
        &mut self,
        _req: &fuser::Request,
        _parent: u64,
        _name: &std::ffi::OsStr,
        _newparent: u64,
        _newname: &std::ffi::OsStr,
        _flags: u32,
        reply: fuser::ReplyEmpty,
    ) {
        println!(
            "u/i: renmae on {_parent} with name {_name:?}; new parent: {_newparent} with new name: {_newname:?}"
        );

        reply.error(ENOSYS);
    }

    fn link(
        &mut self,
        _req: &fuser::Request,
        _ino: u64,
        _newparent: u64,
        _newname: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        println!("u/i: link on ino/{_ino} newparent is: {_newparent}, newname is: {_newname:?}");

        reply.error(ENOSYS);
    }

    fn open(&mut self, _req: &fuser::Request, ino: u64, flags: i32, reply: fuser::ReplyOpen) {
        println!("open(ino: {ino}, flags: {flags:x})");

        let access_mode = flags & O_ACCMODE;
        println!(
            "Access mode: {:?}",
            match access_mode {
                O_RDONLY => "Read-only",
                O_WRONLY => "Write-only",
                O_RDWR => "Read-write",
                _ => "Unknown",
            }
        );

        // Check for unsupported flags (e.g., O_TRUNC)
        if (flags & libc::O_TRUNC) != 0 {
            println!("O_TRUNC not supported!");
            reply.error(libc::EINVAL); // Or handle truncation
            return;
        }

        let fh = self.next_fh();

        self.allocate_fh(fh, ino);

        reply.opened(fh, flags.try_into().unwrap());
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        println!("read ino/{ino} fh/{fh}");
        println!("ino from fh is: {:?}", self.get_ino(fh));

        let dir_ino = self.ino_cache.find_parent(ino);
        println!("ino cache returns: {:?}", dir_ino);

        if dir_ino.is_none() {
            println!("\x1b[31;1mNo parent directory! EIO!\x1b[0m");
            reply.error(EIO);
            return;
        }

        let dir_ino = dir_ino.unwrap();
        let ent = self.fs.get_entity_by_parent_and_block(dir_ino, ino);

        if ent.is_none() {
            // Maybe file is deleted when read is performed idk what to do, let's throw ENOENT then!
            println!("\x1b[31;1mNo entry! ENOENT!\x1b[0m");
            reply.error(ENOENT);
            return;
        }

        let ent = ent.unwrap();

        let mut data = vec![0u8; size as usize];

        self.fs
            .read_contents_by_entity(&ent, &mut data, offset as _)
            .unwrap();

        reply.data(data.as_slice());
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        println!("\x1b[31mwrite\x1b[0m ino/{ino}; fh/{fh}");
        println!("ino from fh is: {:?}", self.get_ino(fh));

        let dir_ino = self.ino_cache.find_parent(ino);
        println!("ino cache returns: {:?}", dir_ino);

        if dir_ino.is_none() {
            println!("\x1b[31;1mNo parent directory! EIO!\x1b[0m");
            reply.error(EIO);
            return;
        }

        let dir_ino = dir_ino.unwrap();
        let ent = self.fs.get_entity_by_parent_and_block(dir_ino, ino);

        if ent.is_none() {
            println!("\x1b[31;1mNo entry! ENOENT!\x1b[0m");
            reply.error(ENOENT);
            return;
        }

        let ent = ent.unwrap();

        println!("Write on: {}", ent.name);

        self.fs
            .write_contents_by_entity(dir_ino, &ent, data, offset.try_into().unwrap());

        reply.written(data.len() as _);
    }

    fn flush(
        &mut self,
        _req: &fuser::Request,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn release(
        &mut self,
        _req: &fuser::Request,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsync(
        &mut self,
        _req: &fuser::Request,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        println!("fsync");

        reply.ok();
    }

    fn opendir(&mut self, _req: &fuser::Request, _ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        println!("opendir {_ino} {_flags}");

        if _ino == 1 {
            let fh = self.next_fh();

            self.allocate_fh(fh, 1);

            reply.opened(fh, _flags.try_into().unwrap());

            return;
        }

        println!("== Other dir!");
        let ent = self.noct_search_by_block(_ino);
        if ent.is_none() {
            println!("\x1b[31;1mNo entry! ENOENT!\x1b[0m");

            reply.error(ENOENT);
            return;
        }
        let ent = ent.unwrap();

        if !ent.is_directory() {
            println!("\x1b[31;1mIs not a directory! ENOENT!\x1b[0m");

            reply.error(ENOENT);
            return;
        }

        let fh = self.next_fh();
        self.allocate_fh(fh, _ino);

        reply.opened(fh, _flags.try_into().unwrap());

        println!("Pushed fh: {}", fh);
    }

    fn readdir(
        &mut self,
        _req: &fuser::Request,
        mut _ino: u64,
        _fh: u64,
        _offset: i64,
        mut reply: fuser::ReplyDirectory,
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
            let result: bool = reply.add(
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

    fn readdirplus(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        reply: fuser::ReplyDirectoryPlus,
    ) {
        println!(
            "[Not Implemented] readdirplus(ino: {:#x?}, fh: {}, offset: {})",
            ino, fh, offset
        );
        reply.error(ENOSYS);
    }

    fn releasedir(
        &mut self,
        _req: &fuser::Request,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsyncdir(
        &mut self,
        _req: &fuser::Request,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn statfs(&mut self, _req: &fuser::Request, _ino: u64, reply: fuser::ReplyStatfs) {
        reply.statfs(0, 0, 0, 0, 0, 512, 255, 0);
    }

    fn setxattr(
        &mut self,
        _req: &fuser::Request,
        _ino: u64,
        _name: &std::ffi::OsStr,
        _value: &[u8],
        _flags: i32,
        _position: u32,
        reply: fuser::ReplyEmpty,
    ) {
        println!("u/i: setxattr on {_ino} with name {_name:?}");
        reply.error(ENOSYS);
    }

    fn access(&mut self, _req: &fuser::Request, _ino: u64, _mask: i32, reply: fuser::ReplyEmpty) {
        println!("access: on ino/{_ino} with mask/{_mask}");

        let parent = self.ino_cache.find_parent(_ino);
        println!("Parent: {parent:?}");

        // Search inode across entire FS (may be slow, but idk what to do without parent ino)
        let a = self.noct_search_by_block(_ino);
        if a.is_none() {
            println!("access failed!");
            reply.error(ENOENT);
            return;
        }

        println!("access succeeded");
        reply.ok();
    }

    fn create(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        println!("Create {name:?} on ino/{parent} with mode(o) {mode:o} and flags(x) {flags:x}");

        let entity = self.fs.create_file(parent, name.to_str().unwrap());

        let fh = self.next_fh();
        self.allocate_fh(fh, entity.start_block);

        self.ino_cache.add(parent, entity.start_block);

        reply.created(
            &DEFAULT_DURATION,
            &self.entity_attrs_to_fuse_attrs(&entity),
            0,
            fh,
            flags as u32 & 0b111,
            // O_RDWR.try_into().unwrap(),
        );
    }

    fn getlk(
        &mut self,
        _req: &fuser::Request,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        _start: u64,
        _end: u64,
        _typ: i32,
        _pid: u32,
        reply: fuser::ReplyLock,
    ) {
        println!("u/i: getlk on {_ino} with fh {_fh}");

        reply.error(ENOSYS);
    }

    fn setlk(
        &mut self,
        _req: &fuser::Request,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        _start: u64,
        _end: u64,
        _typ: i32,
        _pid: u32,
        _sleep: bool,
        reply: fuser::ReplyEmpty,
    ) {
        println!("u/i: setlk on {_ino} with fh {_fh}");

        reply.error(ENOSYS);
    }

    fn bmap(
        &mut self,
        _req: &fuser::Request,
        _ino: u64,
        _blocksize: u32,
        _idx: u64,
        reply: fuser::ReplyBmap,
    ) {
        println!("u/i: bmap on ino/{_ino}, blocksize: {_blocksize}, index: {_idx}");
        reply.error(ENOSYS);
    }

    fn ioctl(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        flags: u32,
        cmd: u32,
        in_data: &[u8],
        out_size: u32,
        reply: fuser::ReplyIoctl,
    ) {
        println!(
            "[Not Implemented] ioctl(ino: {:#x?}, fh: {}, flags: {}, cmd: {}, \
            in_data.len(): {}, out_size: {})",
            ino,
            fh,
            flags,
            cmd,
            in_data.len(),
            out_size,
        );
        reply.error(ENOSYS);
    }

    fn fallocate(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        length: i64,
        mode: i32,
        reply: fuser::ReplyEmpty,
    ) {
        println!(
            "[Not Implemented] fallocate(ino: {:#x?}, fh: {}, offset: {}, \
            length: {}, mode: {})",
            ino, fh, offset, length, mode
        );
        reply.error(ENOSYS);
    }

    fn lseek(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        whence: i32,
        reply: fuser::ReplyLseek,
    ) {
        println!(
            "[Not Implemented] lseek(ino: {:#x?}, fh: {}, offset: {}, whence: {})",
            ino, fh, offset, whence
        );
        reply.error(ENOSYS);
    }

    fn copy_file_range(
        &mut self,
        _req: &Request<'_>,
        ino_in: u64,
        fh_in: u64,
        offset_in: i64,
        ino_out: u64,
        fh_out: u64,
        offset_out: i64,
        len: u64,
        flags: u32,
        reply: fuser::ReplyWrite,
    ) {
        println!(
            "[Not Implemented] copy_file_range(ino_in: {:#x?}, fh_in: {}, \
            offset_in: {}, ino_out: {:#x?}, fh_out: {}, offset_out: {}, \
            len: {}, flags: {})",
            ino_in, fh_in, offset_in, ino_out, fh_out, offset_out, len, flags
        );
        reply.error(ENOSYS);
    }

    fn getxattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        size: u32,
        reply: fuser::ReplyXattr,
    ) {
        println!(
            "[Not Implemented] getxattr(ino: {:#x?}, name: {:?}, size: {})",
            ino, name, size
        );
        reply.error(ENOSYS);
    }

    fn listxattr(&mut self, _req: &Request<'_>, ino: u64, size: u32, reply: fuser::ReplyXattr) {
        println!(
            "[Not Implemented] listxattr(ino: {:#x?}, size: {})",
            ino, size
        );
        reply.error(ENOSYS);
    }

    fn removexattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        println!(
            "[Not Implemented] removexattr(ino: {:#x?}, name: {:?})",
            ino, name
        );
        reply.error(ENOSYS);
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
    let result = fuser::mount2(
        fs,
        &mountpoint,
        &[
            MountOption::FSName("NoctFS".to_owned()),
            MountOption::NoDev,
            MountOption::NoSuid,
            MountOption::Sync,
            MountOption::NoAtime,
            MountOption::RW,
        ],
    );
    std::fs::remove_dir(mountpoint)?;

    println!("Result: {:?}", result);

    Ok(())
}
