import duckdb
import pandas as pd


def init_db():
    conn = duckdb.connect(
        database="/home/alextay96/Desktop/all_workspace/Zhihu-KOL/data/db/zhihu.db",
        read_only=False,
    )
    duckdb.sql("SET GLOBAL pandas_analyze_sample=100000", connection=conn)
    duckdb.sql("SET enable_progress_bar=true", connection=conn)
    duckdb.sql("SET threads TO 6", connection=conn)
    duckdb.commit(conn)
    # all_uploaded_files = glob.glob(
    #     "/home/alextay96/Desktop/all_workspace/Zhihu-KOL/data/s3/**.parquet"
    # )

    # uploaded_df = pd.concat([pd.read_parquet(x) for x in all_uploaded_files])
    # seed_db(uploaded_df, conn)
    return conn


conn = init_db()
# print(
#     duckdb.sql(
#         """
#        Drop table if exists zhihu_answers
#         """,
#         connection=conn,
#     )
# )
print(
    duckdb.sql(
        """
        Create table if not exists zhihu_answers as
        Select distinct *
        FROM raw_zhihu_answers
        """,
        connection=conn,
    )
)
output_file = "/home/alextay96/Desktop/all_workspace/Zhihu-KOL/zhihu_answers_1M.parquet"
print(
    duckdb.sql(
        f"""
        COPY (SELECT * FROM zhihu_answers) TO '{output_file}' (FORMAT PARQUET);
        """,
        connection=conn,
    )
)
df = pd.read_parquet(
    output_file,
)
df.drop_duplicates(subset=["question_id", "answer_id"], inplace=True)
print(df)

df.to_parquet("zhihu_answers_1M_unique.parquet")

# df.to_parquet("/home/alextay96/Desktop/all_workspace/Zhihu-KOL/zhihu_answers_1.parquet")
# print(duckdb.sql("SHOW TABLES", connection=conn))
