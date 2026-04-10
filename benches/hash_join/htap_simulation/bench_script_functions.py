import subprocess
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import re
def parse_result(log_text, table_type):
    data = []
    repair_type = None

    for line in log_text.splitlines():
        line = line.strip()

        if line.startswith("No Repair"):
            repair_type = "No Repair"
            continue
        elif line.startswith("Read Repair"):
            repair_type = "Read Repair"
            continue
        elif line.startswith("Write Repair"):
            repair_type = "Write Repair"
            continue
        
        match = re.match(r'\[(.*?)\]\s+idx:\s+(\d+),\s+tx_id:\s+(\d+),\s+tx_type:\s+(\w+),\s+duration:\s+([0-9\.]+)(ms|s|µs|ns),(.*)', line)
        if match and repair_type:
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
            build_reason = None
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
            build_reason_match = re.search(r'build_for:\s*([A-Za-z]+)', rest)
            if build_reason_match:
                build_reason = build_reason_match.group(1)
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
                'to_ts': to_ts,
                'build_reason': build_reason,
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

def draw_stack_bar(df, title1, classification):
    pivot_df = df.pivot_table(
        index=["table_type", "repair_type"],
        columns="tx_type",
        values="duration_ms",
        aggfunc="sum",
        fill_value=0
    )

    for col in ["InitLoad", "Update", "GbgCollect", "MarkTs", "Probe", "Scan", "DelSc"]:
        cols = pivot_df.columns.tolist()
        if col in cols:
            cols.remove(col)
            cols.append(col)
            pivot_df = pivot_df[cols]

    pivot_df.index = [f"{t}\n{r}" for t, r in pivot_df.index]

    # stacked bar
    ax = pivot_df.plot(
        kind="bar",
        stacked=True,
        figsize=(10, 6)
    )

    plt.ylabel("Duration (ms)")
    plt.title(f"Stacked Duration ({title1} - {classification})")
    plt.xticks(rotation=0)

    handles, labels = ax.get_legend_handles_labels()

    order = pivot_df.columns.tolist()[::-1]
    label_to_handle = dict(zip(labels, handles))
    ax.legend(
        [label_to_handle[l] for l in order],
        order,
        title="tx_type",
        bbox_to_anchor=(1.05, 1),
        loc="upper left"
    )

    plt.tight_layout()
    plt.show()


def parse_result_space(log_text, table_type):
    transalte_dict = {
        "heap": "MONO",
        "naive": "SNAP",
        "par": "EPOCH",
        "chain": "DUAL"
    }
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
        
        pattern = re.compile(
            r"total_space:\s*(\d+),\s*all_versions_space:\s*(\d+),\s*valid_space:\s*(\d+)"
        )

        match = pattern.search(line)
        if match and repair_type:
            total_space = int(match.group(1))
            all_versions_space = int(match.group(2))
            valid_space = int(match.group(3))
            print("total_space:", total_space)
            print("all_versions_space:", all_versions_space)
            print("valid_space:", valid_space)
            table_type = transalte_dict[table_type]
            data.append({
                'table_type': table_type,
                'repair_type': repair_type,
                'total_space': total_space,
                'all_versions_space': all_versions_space,
                'valid_space': valid_space,
            })
    return pd.DataFrame(data)


def run_and_collect_space(bin_path, table_types, base_args, repeat):
    all_dfs = []
    do_once_flag = True
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

            if do_once_flag and table_type == "heap":
                df_once = parse_result(result.stdout, table_type)
                display_all_txns(df_once)
                do_once_flag = False

            print(result.stdout)
            df = parse_result_space(result.stdout, table_type)
            dfs.append(df)
        if dfs:
            # Concatenate all repeated runs
            df_all = pd.concat(dfs, ignore_index=True)
            # Group by (idx, tx_id, tx_type, repair_type, table_type) and average duration_ms
            df_avg = (
                df_all
                .groupby(['table_type', 'repair_type'], as_index=False)
                .agg({
                    'total_space': 'first',
                    'all_versions_space': 'first',
                    'valid_space': 'first'
                })
            )

            all_dfs.append(df_avg)
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        return final_df
    else:
        return pd.DataFrame()


def display_all_txns(df):
    filtered = df[(df["table_type"] == "heap") & (df["repair_type"] == "No Repair")]

    # Group by tx_type and sum duration_ms
    agg = filtered.groupby("tx_type").size().reset_index(name="count")

    agg["percentage"] = agg["count"] / agg["count"].sum() * 100

    display(agg)

def plot_stack_bar_chart(df, title):
    color_map, repair_hatches, repair_types, table_types, unique_idx, x_labels, dict_labels = prepare_plot_data(df)

    groups = []
    for table_type in table_types:
        if table_type in ["chain", "naive"]:
            groups.append((table_type, "Write Repair"))
        else:
            for repair_type in repair_types:
                groups.append((table_type, repair_type))

    print(groups)
    type_and_durations = []
    for i, (table_type, repair_type) in enumerate(groups):
        if table_type in ["chain", "naive"] and repair_type != "Write Repair":
            continue

        df_group = df[(df['table_type'] == table_type) & (df['repair_type'] == repair_type)]

        scan_count = 0

        for idx in unique_idx:
            df_row = df_group[df_group['idx'] == idx]
            duration = df_row['duration_ms'].values[0] if not df_row.empty else 0
            label = dict_labels[idx]
            cur_tx_type = label['tx_type']

            # line = {"table_type": table_type, "repair_type": repair_type, "duration_ms":duration, "tx_type": cur_tx_type, "idx": label["idx"], "optional": label["optional"] }
            line = {"table_type": table_type, "repair_type": repair_type, "duration_ms":duration, "tx_type": cur_tx_type, "idx": label["idx"], "optional": label["optional"] }
            type_and_durations.append(line)
    # print(type_and_durations)
    df = pd.DataFrame(type_and_durations)
    filtered = df[(df["table_type"] == "heap") & (df["repair_type"] == "No Repair")]

    # Group by tx_type and sum duration_ms
    agg = filtered.groupby("tx_type").size().reset_index(name="count")
    display(agg)
    result = (
        df.groupby(["table_type", "repair_type", "tx_type"], as_index=False)
        .agg(duration_ms=("duration_ms", lambda x: x.sum())) #  if x.name[2] == "Update" or x.name[2] == "Probe" else x.mean()
    )
    display(result)

    draw_stack_bar(result, title,  "All Types")

    agg.to_csv(f'output/{title}_wkld.csv', index=False)
    result.to_csv(f'output/{title}_result.csv', index=False)
