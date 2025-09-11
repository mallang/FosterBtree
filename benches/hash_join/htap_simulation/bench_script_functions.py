import subprocess
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import re

def parse_result(log_text, table_type):
    data = []
    repair_type = None
    no_repair_run_count = 0

    for line in log_text.splitlines():
        line = line.strip()

        if line.startswith("No Repair"):
            repair_type = "No Repair"
            no_repair_run_count += 1
            continue
        elif line.startswith("Read Repair"):
            repair_type = "Read Repair"
            continue
        elif line.startswith("Write Repair"):
            repair_type = "Write Repair"
            continue
        
        match = re.match(r'\[(.*?)\]\s+idx:\s+(\d+),\s+tx_id:\s+(\d+),\s+tx_type:\s+(\w+),\s+duration:\s+([0-9\.]+)(ms|s|µs|ns),(.*)', line)
        if match and repair_type:
            if repair_type == "No Repair" and no_repair_run_count == 1:
                continue
            
            _, idx, tx_id, tx_type, duration_value, duration_unit, rest = match.groups()

            duration_value = float(duration_value)
            if duration_unit == 's':
                duration_ms = duration_value * 1000
            elif duration_unit == 'ms':
                duration_ms = duration_value
            elif duration_unit == 'µs':
                duration_ms = duration_value / 1000
            elif duration_unit == 'ns':
                duration_ms = 0.
            else:
                raise ValueError(f"Unknown time unit: {duration_unit}")
            
            count = None
            read_ts = None
            from_ts = None
            to_ts = None
            if 'InitialLoad count' in rest or 'Update count' in rest or 'Probe count' in rest:
                count_match = re.search(r'count:\s*(\d+)', rest)
                if count_match:
                    count = int(count_match.group(1))
            if 'to' in rest and 'from' in rest:
                from_match = re.search(r'from read_ts:\s*(\d+)', rest)
                if from_match:
                    from_ts = int(from_match.group(1))
                
                to_match = re.search(r'to tx_ts:\s*(\d+)', rest)
                if to_match:
                    to_ts = int(to_match.group(1))
            elif 'read_ts' in rest:
                read_ts_match = re.search(r'read_ts:\s*(\d+)', rest)
                if read_ts_match:
                    read_ts = int(read_ts_match.group(1))
            data.append({
                'table_type': table_type,
                'repair_type': repair_type,
                'idx': int(idx),
                'tx_id': int(tx_id),
                'tx_type': tx_type,
                'duration_ms': duration_ms,
                'count': count,
                'read_ts': read_ts,
                'from_ts': from_ts,
                'to_ts': to_ts
            })
    return pd.DataFrame(data)


def run_and_collect(bin_path, table_types, base_args, repeat):
    all_dfs = []
    for table_type in table_types:
        print(f"Running for table_type={table_type} with {repeat} repeats...")
        dfs = []
        for trail in range(repeat):
            print(f"   - Trail {trail + 1}/{repeat}")

            args = [str(bin_path)] + base_args + ["--table-type", table_type]
            print(args)

            result = subprocess.run(
                args, 
                stdout=subprocess.PIPE,
                stderr = subprocess.PIPE,
                text=True,
            )

            print(result.stdout)
            df = parse_result(result.stdout, table_type)
            dfs.append(df)
        if dfs:
            # Concatenate all repeated runs
            df_all = pd.concat(dfs, ignore_index=True)
            # Group by (idx, tx_id, tx_type, repair_type, table_type) and average duration_ms
            df_avg = (
                df_all
                .groupby(['idx', 'tx_id', 'tx_type', 'repair_type', 'table_type'], as_index=False)
                .agg({
                    'duration_ms': 'mean',
                    'count': 'first',
                    'read_ts': 'first',
                    'from_ts': 'first',
                    'to_ts': 'first'
                })
            )

            all_dfs.append(df_avg)
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        return final_df
    else:
        return pd.DataFrame()


def prepare_plot_data(df):
    prop_cycler = plt.rcParams['axes.prop_cycle']
    colors = prop_cycler.by_key()['color']

    table_types = sorted(df['table_type'].unique())
    color_map = {table_type: colors[i % len(colors)] for i, table_type in enumerate(table_types)}

    repair_hatches = {
        "Write Repair": "",
        "Read Repair": "///",
        "No Repair": "",
    }
    repair_types = ["No Repair", "Read Repair", "Write Repair"]

    unique_idx = sorted(df['idx'].unique())

    x_labels = []
    dict_labels = []
    for idx in unique_idx:
        row = df[df['idx'] == idx].iloc[0]
        optional = ""
        if row['tx_type'] in ["InitLoad", "Update"] and pd.notnull(row['count']):
            count = int(row['count'])
            if count >= 1_000_000:
                value = count / 1_000_000
                count_str = f"{value:.1f}".rstrip('0').rstrip('.') + "M"
            elif count >= 1_000:
                value = count / 1_000
                count_str = f"{value:.1f}".rstrip('0').rstrip('.') + "K"
            else:
                count_str = str(count)
            optional = f"cnt: {count_str}"
        elif row['tx_type'] in ["Scan", "MarkTs", "GbgCollect"] and pd.notnull(row['read_ts']):
            optional = f"ts: {int(row['read_ts'])}"
        elif row['tx_type'] in ["DeltaScan"] and pd.notnull(row['from_ts']) and pd.notnull(row['to_ts']):
            optional = f"ts: ({int(row['from_ts'])}, {int(row['to_ts'])})"
        elif row['tx_type'] in ["Probe"]:
            assert pd.notnull(row['count']) and pd.notnull(row['read_ts'])
            count = int(row['count'])
            if count >= 1_000_000:
                value = count / 1_000_000
                count_str = f"{value:.1f}".rstrip('0').rstrip('.') + "M"
            elif count >= 1_000:
                value = count / 1_000
                count_str = f"{value:.1f}".rstrip('0').rstrip('.') + "K"
            else:
                count_str = str(count)
            optional = f"cnt: {count_str}, ts: {int(row['read_ts'])}"
        tx_type = row['tx_type']
        if row['tx_type'] in ["DeltaScan"]:
            tx_type = "DelSc"
        label = f"{idx}\n{tx_type}\n{optional}"
        label_dict = {"idx": idx, "tx_type": tx_type, "optional":optional}
        x_labels.append(label)
        dict_labels.append(label_dict)

    return color_map, repair_hatches, repair_types, table_types, unique_idx, x_labels, dict_labels
