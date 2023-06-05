import json
import pandas as pd
import numpy as np
import scipy.stats as s
from tqdm import tqdm

pattern_list = ["Hammer", "Long-Legged-Doji", "Gravestone_Doji", "Dragonfly_Doji", "Belt_Hold_Bearish",
                "Belt_Hold_Bullish"]
base_line = []
pattern_line = dict()
for pattern in pattern_list:
    pattern_line[pattern] = []
for i in tqdm(range(1, 21)):
    with open(f"C:/Users/Franck/base_line_{i}.json", "r") as f:
        base_line_data = json.load(f)
        base_line += base_line_data
    with open(f"C:/Users/Franck/pattern_line_{i}.json", "r") as f:
        pattern_line_data = json.load(f)
        for pattern in pattern_list:
            pattern_line[pattern] += pattern_line_data[pattern]
res = []
for pattern, line in pattern_line.items():
    res.append(
        [pattern, sum(line), np.mean(line), sum(base_line), np.mean(base_line), s.ttest_ind(line, base_line).pvalue])
df = pd.DataFrame(res, columns=["pattern_name", "count", "pattern_mean", "base_line_count", "base_line_mean",
                                "pvalue_of_t_ttest"])
df.to_excel("Pattern_reversal_stats_of_zipped_barchat.xlsx")