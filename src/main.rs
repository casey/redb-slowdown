use {clap::Parser, std::fs};

const TABLE: redb::TableDefinition<u64, [u8]> = redb::TableDefinition::new("TABLE");

#[derive(Parser)]
struct Arguments {
  #[clap(long)]
  lmdb: bool,
  #[clap(long)]
  size: usize,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
  env_logger::Builder::new()
    .filter_level(log::LevelFilter::Info)
    .init();

  let args = Arguments::parse();

  if args.lmdb {
    use lmdb::Transaction;
    fs::create_dir_all("env.lmdb").unwrap();

    let env = lmdb::Environment::new()
      .set_map_size(args.size)
      .open("env.lmdb".as_ref())?;

    let db = env.open_db(None)?;

    for i in 0u64..1000000 {
      log::info!("{i}");

      let mut tx = env.begin_rw_txn().unwrap();

      let value = vec![0; 256];

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
    let db = unsafe { redb::Database::create("db.redb", args.size)? };

    for i in 0..1000000 {
      log::info!("{i}");
      let tx = db.begin_write()?;

      let mut table = tx.open_table(&TABLE)?;

      let value = vec![0; 256];

      for j in 0..10 {
        table.insert(&(i * 10 + j), &value)?;
      }

      tx.commit()?;
    }
  }
  Ok(())
}
