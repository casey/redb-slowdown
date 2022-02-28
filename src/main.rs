use {clap::Parser, std::fs};

const TABLE: redb::TableDefinition<u64, [u8]> = redb::TableDefinition::new("TABLE");

#[derive(Parser)]
struct Arguments {
  #[clap(long)]
  lmdb: bool,
}

// lmdb
// real    0m16.934s
// user    0m0.928s
// sys     0m5.370s
//
// redb
// real    2m11.401s
// user    0m16.913s
// sys     0m54.395s

fn main() -> Result<(), Box<dyn std::error::Error>> {
  env_logger::Builder::new()
    .filter_level(log::LevelFilter::Info)
    .init();

  let args = Arguments::parse();

  let iterations = 750;

  let value = vec![0; 1 << 20];

  let size = 15 << 30;

  fs::remove_file("db.redb").ok();
  fs::remove_dir_all("env.lmdb").ok();

  if args.lmdb {
    use lmdb::Transaction;

    fs::create_dir_all("env.lmdb").unwrap();

    let env = lmdb::Environment::new()
      .set_map_size(size)
      .open("env.lmdb".as_ref())?;

    let db = env.open_db(None)?;

    for i in 0u64..iterations {
      log::info!("{i}");

      let mut tx = env.begin_rw_txn().unwrap();

      for j in 0..10 {
        tx.put(
          db,
          &(i * 10 + j).to_be_bytes(),
          &value,
          lmdb::WriteFlags::empty(),
        )?;
      }

      tx.commit()?;
    }
  } else {
    let db = unsafe { redb::Database::create("db.redb", size)? };

    for i in 0..iterations {
      log::info!("{i}");

      let tx = db.begin_write()?;

      let mut table = tx.open_table(TABLE)?;

      for j in 0..10 {
        table.insert(&(i * 10 + j), &value)?;
      }

      tx.commit()?;
    }
  }

  Ok(())
}
