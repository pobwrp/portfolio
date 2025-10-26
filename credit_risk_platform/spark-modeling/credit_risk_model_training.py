"""Random forest credit-risk trainer used by the Airflow showcase DAG.

The script expects a curated feature table where ``label_default`` is a
binary indicator and ``snapshot_date`` represents the data cut-off.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import DataFrame, SparkSession, functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--feature_store_path", required=True)
    parser.add_argument("--model_registry_path", required=True)
    parser.add_argument("--metrics_path", required=True)
    parser.add_argument("--training_window_months", type=int, default=6)
    parser.add_argument("--sampling_ratio", type=float, default=1.0)
    parser.add_argument("--run_tag", default="manual")
    return parser.parse_args()


def filter_recent_observations(df: DataFrame, months: int) -> DataFrame:
    cutoff = F.add_months(F.current_date(), -months)
    return df.filter(F.col("snapshot_date") >= cutoff)


def build_feature_pipeline(df: DataFrame) -> Pipeline:
    categorical_columns = [
        "customer_segment",
        "occupation",
        "loan_subtype",
        "region",
    ]

    numeric_columns = [col for col in df.columns if col not in {"label_default", "snapshot_date", *categorical_columns}]

    stages = [StringIndexer(inputCol=column, outputCol=f"{column}_idx", handleInvalid="keep") for column in categorical_columns]
    assembler = VectorAssembler(
        inputCols=[*(f"{column}_idx" for column in categorical_columns), *numeric_columns],
        outputCol="features",
        handleInvalid="keep",
    )
    stages.append(assembler)
    return Pipeline(stages=stages)


def rebalance(train_df: DataFrame, ratio: float) -> DataFrame:
    if ratio <= 1.0:
        return train_df

    positive = train_df.filter(F.col("label_default") == 1)
    negative = train_df.filter(F.col("label_default") == 0)
    desired = int(ratio * positive.count())
    sampled_negative = negative.orderBy(F.rand()).limit(desired)
    return positive.unionByName(sampled_negative)


def train_model(df: DataFrame, ratio: float) -> tuple[RandomForestClassifier, DataFrame, DataFrame]:
    pipeline = build_feature_pipeline(df)
    transformed = pipeline.fit(df).transform(df)

    train_df, test_df = transformed.randomSplit([0.7, 0.3], seed=42)
    train_df = rebalance(train_df, ratio)

    classifier = RandomForestClassifier(
        labelCol="label_default",
        featuresCol="features",
        maxDepth=8,
        numTrees=200,
        subsamplingRate=0.8,
    )
    model = classifier.fit(train_df)
    predictions = model.transform(test_df)
    return model, pipeline, predictions


def evaluate(predictions: DataFrame) -> dict[str, float]:
    evaluator = BinaryClassificationEvaluator(
        labelCol="label_default",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )
    auc = evaluator.evaluate(predictions)

    matrix = (
        predictions.groupBy("label_default", "prediction")
        .count()
        .collect()
    )
    counts = {(row["label_default"], row["prediction"]): float(row["count"]) for row in matrix}

    true_positive = counts.get((1.0, 1.0), 0.0)
    false_negative = counts.get((1.0, 0.0), 0.0)
    false_positive = counts.get((0.0, 1.0), 0.0)

    recall = true_positive / max(true_positive + false_negative, 1.0)
    precision = true_positive / max(true_positive + false_positive, 1.0)

    return {
        "auc": auc,
        "precision": precision,
        "recall": recall,
    }


def persist_artifacts(model, pipeline, metrics: dict[str, float], args: argparse.Namespace) -> None:
    registry_path = Path(args.model_registry_path) / args.run_tag
    metrics_dir = Path(args.metrics_path) / args.run_tag

    registry_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_dir.parent.mkdir(parents=True, exist_ok=True)

    model.write().overwrite().save(str(registry_path / "model"))
    pipeline.write().overwrite().save(str(registry_path / "pipeline"))

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    metrics_df = spark.createDataFrame(
        [(args.run_tag, metrics["precision"], metrics["recall"], metrics["auc"])],
        ["run_tag", "precision", "recall", "auc"],
    )
    metrics_df.write.mode("overwrite").json(str(metrics_dir))


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("credit_risk_model_training").getOrCreate()

    df = spark.read.parquet(args.feature_store_path)
    df = filter_recent_observations(df, args.training_window_months)
    df = df.dropna(subset=["label_default"])

    model, pipeline, predictions = train_model(df, args.sampling_ratio)
    metrics = evaluate(predictions)
    persist_artifacts(model, pipeline, metrics, args)

    spark.stop()


if __name__ == "__main__":
    main()
