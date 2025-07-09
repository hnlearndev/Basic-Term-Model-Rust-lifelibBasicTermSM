# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "marimo",
#     "matplotlib==3.10.3",
#     "numpy==2.3.1",
#     "pandas==2.3.1",
#     "polars==1.31.0",
#     "pyarrow==20.0.0",
#     "pygam==0.9.1",
#     "scikit-learn==1.7.0",
#     "scipy==1.16.0",
#     "seaborn==0.13.2",
# ]
# ///

import marimo

__generated_with = "0.14.10"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import pandas as pd
    import seaborn as sns

    import numpy as np
    import matplotlib.pyplot as plt
    from sklearn.preprocessing import SplineTransformer
    from sklearn.linear_model import Ridge
    from sklearn.pipeline import make_pipeline
    import os
    return Ridge, SplineTransformer, make_pipeline, mo, np, pd, pl, plt, sns


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Abstract

    This notebook shows how the premium rates are smoothed after being calculated using Rust model.

    The intension at first is to used Rust for analysis.
    However, at the time of production, Rust language compilation nature takes significant time to produce the output in reactive notebook environment.
    On another hand, Python reactive notebook is more advanced and packed with featured.
    Hence, Python is used for post analysis.

    What is done in this notebook?
    After premium rate is calculated using Rust model, the premium rates are still jumpy at several model points.
    This notebook shows how the premium rate is smoothed using common p-sline technique.

    In practise, this step is usually performed manually with interpolation. However, it is not recommended as not being reproducible and labor-intensive.

    ## Crude premium rate

    """
    )
    return


@app.cell
def _(pl):
    # Load the DataFrame from the Parquet file
    current_dir = os.path.dirname(__file__)
    parquet_path = os.path.join(current_dir, "..", "..", "results", "s_test", "run_0", "projected_df.parquet")
    df = pl.read_parquet(parquet_path)

    # Transform data to obtain premium rate
    prem_rate_df = (
        df.lazy()
        .with_columns(
            (pl.col("prem_pp") / pl.col("sum_insured") * pl.lit(1000.0)).alias(
                "prem_rate"
            )
        )
        .filter(pl.col("t") == 0)
        .group_by(["age", "term"])
        .agg(pl.col("prem_rate").mean().alias("ave_prem_rate"))
        .sort(["term", "age"])
        .collect()
    )

    prem_rate_df
    return (prem_rate_df,)


@app.cell
def _(mo):
    mo.md(r"""As observed from the scatter plots, there are several points that are not smoothed aka jumpy.
          We expect that the premium rate will gradually increase with positive slope as the age increases.""")
    return


@app.cell
def _(plt, prem_rate_df, sns):
    # Convert to pandas for seaborn plotting
    prem_rate_pd = prem_rate_df.to_pandas()

    # Plot using seaborn
    plt.figure(figsize=(8, 5))
    sns.lineplot(data=prem_rate_pd, x="age", y="ave_prem_rate", hue="term", marker="o")
    plt.xlabel("Age")
    plt.ylabel("Average Premium Rate")
    plt.title("Average Premium Rate by Age and Term")
    plt.grid(True)
    plt.tight_layout()
    plt.show()
    return (prem_rate_pd,)


@app.cell
def _(mo):
    mo.md(r"""## Smooth the crude premium rate""")
    return


@app.cell
def _(Ridge, SplineTransformer, make_pipeline, np, plt, prem_rate_pd):
    plt.figure(figsize=(8, 5))

    # To store smoothed values
    smoothed_points = []

    for term, group in prem_rate_pd.groupby("term"):
        #
        group = group.sort_values("age")
        X = group["age"].values.reshape(-1, 1)
        y = group["ave_prem_rate"].values

        # Spline smoothing pipeline (degree=3 for cubic, n_knots=8 for smoothness)
        n_knots = min(8, len(np.unique(X)))
        model = make_pipeline(
            SplineTransformer(degree=3, n_knots=n_knots, include_bias=False),
            Ridge(alpha=0.0)
        )
        model.fit(X, y)

        # Predict at each observed age
        y_smooth = model.predict(X)

        # Store smoothed values
        group_smoothed = group.copy()
        group_smoothed["smoothed_prem_rate"] = y_smooth
        smoothed_points.append(group_smoothed)

        # Plot smooth curve and scatter
        XX = np.linspace(X.min(), X.max(), 200).reshape(-1, 1)
        plt.plot(XX, model.predict(XX), label=f"Term {term}", linewidth=1)
        plt.scatter(X, y, s=20, alpha=0.5)

    plt.xlabel("Age")
    plt.ylabel("Average Premium Rate")
    plt.title("Smoothed Average Premium Rate by Age and Term (Spline)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()
    return (smoothed_points,)


@app.cell
def _(mo):
    mo.md(r"""After creating smooth curve for each term, we obtained values of the smooth curve""")
    return


@app.cell
def _(pd, smoothed_points):
    # Concatenate all smoothed groups into a single DataFrame
    smoothed_df = pd.concat(smoothed_points, ignore_index=True)

    smoothed_df
    return (smoothed_df,)


@app.cell
def _(pl, smoothed_df):
    # Convert to Polars DataFrame
    smoothed_pl = pl.from_pandas(smoothed_df)

    # For each term, sort by age and compute the difference with the next value
    smoothed_pl = (
        smoothed_pl
        .sort(["term", "age"])
        .with_columns(
            (pl.col("smoothed_prem_rate").shift(-1) - pl.col("smoothed_prem_rate"))
            .over("term")
            .alias("diff_to_next")
        )
    )

    smoothed_pl
    return (smoothed_pl,)


@app.cell
def _(pl, smoothed_pl):
    # Count how many negative values in diff_to_next
    tol = 1e-4
    negative_count = smoothed_pl.filter(pl.col("diff_to_next") < -tol).height
    negative_count
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    As observed there is still one points that is has a negative slope. We can either iterate over different subset of variable to smooth out the curve.

    However, I make an objective decision if tolerance is small enough, it will be ignored. Else, we will obtain the rate from the previous period to make the difference 0.
    """
    )
    return


if __name__ == "__main__":
    app.run()
