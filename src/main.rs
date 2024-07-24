use reth_db::{open_db_read_only, mdbx::DatabaseArguments, models::client_version::ClientVersion};

use reth_provider::{BlockReaderIdExt, StateProviderFactory, ProviderFactory, TransactionVariant,
    providers::{BlockchainProvider, StaticFileProvider}
};

use reth_chainspec::MAINNET;

use reth_primitives::{
    BlockId, U256
};

use reth_revm::database::StateProviderDatabase;

use reth_evm_ethereum::execute::EthExecutorProvider;

use reth_blockchain_tree::{
    BlockchainTree, BlockchainTreeConfig, ShareableBlockchainTree, TreeExternals,
};

use reth_beacon_consensus::EthBeaconConsensus;

use reth_consensus::Consensus;

use revm_interpreter::{
    parallel, print_records, start_channel
};

use std::{fs::OpenOptions, sync::Mutex, time::Duration};
use std::{path::Path, time::Instant};
use std::sync::Arc;
use std::fs::File;
use csv::Error;
use std::thread;

// pub mod contract_runner;
// use contract_runner::run_contract_code;

// #[derive(Parser, Debug)]


fn run_block() -> Result<(), Error> {
    // Read Database Info
    // written in bin/reth/src/commands/debug_cmd/build_block.rs (from line 147)

    let db_path_str = String::from("/home/user/common/docker/volumes/eth-docker_reth-el-data/_data/db");
    let db_path = Path::new(&db_path_str);
    let db = Arc::new(open_db_read_only(&db_path, DatabaseArguments::new(ClientVersion::default())).unwrap());

    let static_files_path_str = String::from("/home/user/common/docker/volumes/eth-docker_reth-el-data/_data/static_files");
    let static_file_path = Path::new(&static_files_path_str).to_path_buf();
    let static_file_provider = StaticFileProvider::read_only(static_file_path).unwrap();

    let chain_spec = MAINNET.clone();

    let provider_factory = ProviderFactory::new(db.clone(), chain_spec.clone(), static_file_provider);

    let consensus: Arc<dyn Consensus> = Arc::new(EthBeaconConsensus::new(chain_spec.clone()));

    let tree_externals = TreeExternals::new(
        provider_factory.clone(),
        Arc::clone(&consensus),
        EthExecutorProvider::mainnet(),
    );
    let tree = BlockchainTree::new(tree_externals, BlockchainTreeConfig::default(), None).unwrap();
    let blockchain_tree = Arc::new(ShareableBlockchainTree::new(tree));

    let blockchain_db =
    BlockchainProvider::new(provider_factory.clone(), blockchain_tree.clone()).unwrap();


    // 创建一个通道
    let _ = start_channel();
    let _ = print_records(); //multi writer
    let _ = print_records(); //multi writer
    let _ = print_records(); //multi writer


    //let mut total_exec_diff = Duration::ZERO;
    let start_time = Instant::now();

    // Execute Block by block number
    let mut round_num = 0;
    let split: u64 = 1; //Bian Add 分割文件
    // let gas_used_sum = 0;


    let file = File::open("./block_range.csv")?;
    let mut reader = csv::ReaderBuilder::new().has_headers(false).from_reader(file);

    for result in reader.records() {

        //Brian add
        if round_num%split == 0 {
            let output_path1: String = format!("./output/{}.log", round_num);
            let output_path2 = format!("./output/{}.log", round_num);
            File::create_new(output_path1).unwrap();
            let f2: File = OpenOptions::new().append(true).open(output_path2).unwrap();
            unsafe { parallel::WRITE_PATH_VEC.push(Mutex::new(f2)) }; //所有权变更吗？
        }

        let record = result?;
        let new_block_num = record[0].parse::<u64>().unwrap();

        let old_block_num = new_block_num - 1;
        let new_block = blockchain_db.block_with_senders_by_id(BlockId::from(new_block_num), TransactionVariant::WithHash).unwrap().unwrap();

        let state_provider = blockchain_db.history_by_block_number(old_block_num).unwrap();
        let state_provider_db = StateProviderDatabase::new(state_provider);

        let mut executor = EthExecutorProvider::mainnet().eth_executor(state_provider_db);

        // let mut executor = EthBlockExecutor::new(chain_spec.clone(), evm_config, StateProviderDatabase::new(state_provider));

        // let result = executor.execute_and_verify_receipt(&new_block, U256::ZERO, None).unwrap();

        //let exec_start_time = Instant::now();

        executor.execute_without_verification(&new_block, U256::ZERO).unwrap();

        //let exec_end_time = Instant::now();
        //let exec_diff = exec_end_time.duration_since(exec_start_time);
        //total_exec_diff += exec_diff;

        // let stat = executor.stats();
        // let result = executor.take_output_state();
        // println!("Show result: {:?}", result);


        round_num += 1;
        // gas_used_sum += gas_used;

        eprint!("{:?}\n", round_num);
    }
    parallel::wait(round_num);

    let end_time = Instant::now();

    // 確保channel能完成所有工作
    //thread::sleep(Duration::from_secs(3));

    // 打印每個opcode運行總時間
    //print_records();

    let diff = end_time.duration_since(start_time);
    eprintln!("Overall Duration Time is {:?} s", diff.as_secs_f64());
    //eprintln!("Total Execution Time is {:?} s\n", total_exec_diff.as_secs_f64());


    // let gas_per_ms = gas_used_sum / exec_time_sum.as_millis();
    // println!("Total Gas Used is {:?} \nTotal Execution Time is {:?}\n Gas Used per millisecond is {:?}", gas_used_sum, exec_time_sum, gas_per_ms);
    Ok(())
}


fn main() {
    run_block().unwrap();
    // // run_contract_code();
    // run_precompile_hash()

    //let counter = Arc::new(std::sync::Mutex::new(0));
}
