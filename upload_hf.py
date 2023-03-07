from datasets import Dataset

ds = Dataset.from_parquet("dataset_03_01.parquet")
ds.push_to_hub("wangrui6/Zhihu-KOL")

# Push code with more controls
# from huggingface_hub import HfApi
# api = HfApi()
# api.upload_file(
#     path_or_fileobj="zhihu_roundtable_02_28.parquet",
#     path_in_repo="data/training_data_001.parquet",
#     repo_id="wangrui6/Zhihu-KOL",
#     repo_type="dataset",
# )
