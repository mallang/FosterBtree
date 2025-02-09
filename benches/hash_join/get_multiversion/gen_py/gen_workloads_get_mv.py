import os
import yaml
import subprocess

def main():
    # workload_dir = '../workload' # directory for CSV files
    # gen_txs_script = 'gen_txs.py'  # Path to gen_txs.py
    # csv_dir = '../csv'  # directory for CSV files
    workload_dir = 'workload' # directory for CSV files
    gen_txs_script = './gen_py/gen_get_multiversion_txs.py'  # Path to gen_txs.py
    csv_dir = 'csv'  # directory for CSV files

    for config_file in os.listdir(workload_dir):
        if config_file.endswith('.yaml') or config_file.endswith('.yml'):
            
            config_path = os.path.join(workload_dir, config_file)
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            step = config.get('step', 20)
            for ratio in range(0, 101, step):
                workload_name = f'hget_ratio{ratio:03}'
                # paths for txs and ops files
                txs_file = os.path.join(csv_dir, f'{workload_name}_txs.csv')
                ops_file = os.path.join(csv_dir, f'{workload_name}_ops.csv')
                data_file = os.path.join(csv_dir, 'data_multiversion.csv')

                # command-line arguments
                args = [
                    'python3', gen_txs_script,
                    '--data_file', data_file,
                    '--txs_file', txs_file,
                    '--ops_file', ops_file,
                    '--num_transactions', str(config.get('num_transactions', 10)),
                    '--min_cmds_per_tx', str(config.get('min_cmds_per_tx', 5)),
                    '--max_cmds_per_tx', str(config.get('max_cmds_per_tx', 10)),
                    '--history_get_ratio', str(ratio/100.0),
                ]

                # Run gen_txs.py with the arguments
                subprocess.run(args, check=True, cwd='.')

if __name__ == '__main__':
    main()