# scripts/spectral_lead_lag_analysis.py
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

'''
Run: 
python scripts/spectral_analysis.py \
  --S_parquet results/leadlag_stage2_gvkey/S_h1.parquet \
  --C_parquet results/leadlag_stage2_gvkey/C_h1.parquet \
  --edge_scores_csv results/leadlag_stage2_gvkey/edge_scores_h1.csv \
  --outdir results/leadlag_spectral_h1 \
  --do_permtest --n_perm 1000 --seed 42 \
  --embed_plot
'''
# ----------------------------
# Utilities
# ----------------------------
def _ensure_square_same_index(S: pd.DataFrame) -> pd.DataFrame:
    """Ensure S is square and aligned on the same sorted node list for rows/cols."""
    if not isinstance(S.index, pd.Index) or not isinstance(S.columns, pd.Index):
        raise ValueError("S must have index and columns as pandas Index.")
    nodes = sorted(set(map(str, S.index)).union(set(map(str, S.columns))))
    S = S.copy()
    S.index = S.index.map(str)
    S.columns = S.columns.map(str)
    return S.reindex(index=nodes, columns=nodes, fill_value=0.0)


def hermitian_from_skew(S: np.ndarray) -> np.ndarray:
    """
    For real skew-symmetric S, define Hermitian adjacency:
        A_tilde = i * S
    This is Hermitian => real eigenvalues; use eigh/eigvalsh.
    """
    return 1j * S.astype(float, copy=False)


def eigendecompose_hermitian(A: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """
    Eigendecompose Hermitian A using eigh:
      returns (eigvals_desc, eigvecs_desc) with eigvecs as columns.
    """
    w, V = np.linalg.eigh(A)  # ascending
    idx = np.argsort(w)[::-1]
    return w[idx], V[:, idx]


def _permute_skew_upper_triangle(S: np.ndarray, rng: np.random.Generator) -> np.ndarray:
    """
    Permute the upper-triangular entries of S (excluding diagonal), then antisymmetrize.

    Produces a skew-symmetric matrix with the same multiset of upper-tri values,
    but randomized across node pairs.
    """
    n = S.shape[0]
    iu = np.triu_indices(n, k=1)
    vals = S[iu].copy()
    rng.shuffle(vals)

    Snull = np.zeros_like(S, dtype=float)
    Snull[iu] = vals
    Snull[(iu[1], iu[0])] = -vals
    return Snull


def permutation_test_max_eig(
    S: np.ndarray,
    *,
    n_perm: int = 500,
    seed: int = 0,
) -> Tuple[float, float, np.ndarray]:
    """
    Permutation test using statistic lambda_max(A_tilde), where A_tilde = iS.

    Returns: (obs_maxeig, p_value, null_maxeig_array)
    """
    rng = np.random.default_rng(seed)

    # observed
    obs_maxeig = float(np.max(np.linalg.eigvalsh(hermitian_from_skew(S))).real)

    # null
    null_maxeig = np.empty(n_perm, dtype=float)
    for b in range(n_perm):
        Snull = _permute_skew_upper_triangle(S, rng)
        null_maxeig[b] = float(np.max(np.linalg.eigvalsh(hermitian_from_skew(Snull))).real)

    # right-tail p-value with +1 smoothing
    pval = (1.0 + np.sum(null_maxeig >= obs_maxeig)) / (n_perm + 1.0)
    return obs_maxeig, float(pval), null_maxeig


def _plot_scree(eigvals: np.ndarray, outpath: Path, top_k: int = 50) -> None:
    k = min(top_k, eigvals.size)
    x = np.arange(1, k + 1)
    y = eigvals[:k]
    plt.figure()
    plt.plot(x, y, marker="o", linestyle="-")
    plt.xlabel("Eigenvalue rank")
    plt.ylabel("Eigenvalue of $\\tilde{A}= iS$")
    plt.title("Spectrum (top eigenvalues)")
    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()


def _plot_null_hist(null_vals: np.ndarray, obs: float, outpath: Path) -> None:
    plt.figure()
    plt.hist(null_vals, bins=40)
    plt.axvline(obs, linewidth=2)
    plt.xlabel("Max eigenvalue under permutation")
    plt.ylabel("Count")
    plt.title("Permutation null distribution for $\\lambda_{\\max}(\\tilde{A})$")
    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()


def _plot_embedding_scatter(
    eigvecs: np.ndarray,
    nodes: list[str],
    outpath: Path,
    dim1: int = 1,
    dim2: int = 2,
    max_points: int = 2000,
    seed: int = 0,
) -> None:
    """
    Scatter plot using Re/Im parts of selected eigenvector(s).
    For Hermitian A, eigenvectors are complex. A simple 2D embedding is:
      x = Re(v_dim1), y = Re(v_dim2)
    (You can swap to Im parts if you prefer.)
    """
    rng = np.random.default_rng(seed)
    n = len(nodes)
    idx = np.arange(n)
    if n > max_points:
        idx = rng.choice(idx, size=max_points, replace=False)

    v1 = eigvecs[:, dim1 - 1]
    v2 = eigvecs[:, dim2 - 1]
    x = np.real(v1[idx])
    y = np.real(v2[idx])

    plt.figure()
    plt.scatter(x, y, s=8)
    plt.xlabel(f"Re(v{dim1})")
    plt.ylabel(f"Re(v{dim2})")
    plt.title("Spectral embedding (subset if large)")
    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()


# ----------------------------
# Main
# ----------------------------
def main():
    ap = argparse.ArgumentParser(description="Spectral analysis for lead-lag skew matrix S.")
    ap.add_argument("--S_parquet", type=str, required=True, help="Path to S_h1.parquet (or similar).")
    ap.add_argument("--C_parquet", type=str, default=None, help="Optional path to C_h1.parquet (for metadata).")
    ap.add_argument("--edge_scores_csv", type=str, default=None, help="Optional edge scores CSV (for reference).")
    ap.add_argument("--outdir", type=str, default="results/leadlag_spectral")
    ap.add_argument("--top_k", type=int, default=50, help="How many top eigenvalues to save/plot.")
    ap.add_argument("--do_permtest", action="store_true")
    ap.add_argument("--n_perm", type=int, default=500)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--embed_plot", action="store_true", help="Also save a 2D embedding scatter plot.")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # Load S
    S_df = pd.read_parquet(args.S_parquet)
    S_df = _ensure_square_same_index(S_df)

    # Basic sanity checks
    S_arr = S_df.to_numpy(dtype=float)
    skew_err = np.max(np.abs(S_arr + S_arr.T))
    (outdir / "sanity.txt").write_text(
        f"max|S + S^T| = {skew_err:.6g}\n"
        f"shape = {S_arr.shape}\n"
    )

    nodes = list(S_df.index)

    # Build Hermitian adjacency and eigendecompose
    A_tilde = hermitian_from_skew(S_arr)
    eigvals, eigvecs = eigendecompose_hermitian(A_tilde)

    # Save eigenvalues
    k = min(args.top_k, eigvals.size)
    eigvals_df = pd.DataFrame({"rank": np.arange(1, eigvals.size + 1), "eigval": eigvals})
    eigvals_df.to_csv(outdir / "eigvals_Atilde.csv", index=False)

    # Save top-k eigenvectors (complex; store Re and Im)
    topV = eigvecs[:, :k]
    eigvecs_re = pd.DataFrame(np.real(topV), index=nodes, columns=[f"Re_v{j}" for j in range(1, k + 1)])
    eigvecs_im = pd.DataFrame(np.imag(topV), index=nodes, columns=[f"Im_v{j}" for j in range(1, k + 1)])
    eigvecs_re.to_parquet(outdir / "eigvecs_top_re.parquet")
    eigvecs_im.to_parquet(outdir / "eigvecs_top_im.parquet")

    # Plots
    _plot_scree(eigvals, outdir / "scree_top_eigs.png", top_k=args.top_k)

    # Optional embedding plot
    if args.embed_plot and eigvals.size >= 2:
        _plot_embedding_scatter(
            eigvecs=eigvecs,
            nodes=nodes,
            outpath=outdir / "embedding_Re_v1_vs_Re_v2.png",
            dim1=1,
            dim2=2,
            seed=args.seed,
        )

    # Permutation test (optional)
    perm_summary = ""
    if args.do_permtest:
        obs, pval, null_vals = permutation_test_max_eig(S_arr, n_perm=args.n_perm, seed=args.seed)
        pd.DataFrame({"null_maxeig": null_vals}).to_csv(outdir / "perm_null_maxeig.csv", index=False)
        _plot_null_hist(null_vals, obs, outdir / "perm_null_maxeig_hist.png")
        perm_summary = f"obs_maxeig={obs:.6g}\np_value={pval:.6g}\n"
        (outdir / "perm_test.txt").write_text(perm_summary)

    # Optional: copy metadata files into outdir for convenience
    if args.C_parquet:
        C_df = pd.read_parquet(args.C_parquet)
        C_df.to_parquet(outdir / "C_copy.parquet")
    if args.edge_scores_csv:
        es = pd.read_csv(args.edge_scores_csv)
        es.to_csv(outdir / "edge_scores_copy.csv", index=False)

    # Console summary
    print(f"[S] shape={S_arr.shape}, max|S+S^T|={skew_err:.3g}")
    print(f"[eig] top 10 eigenvalues of A_tilde=iS: {eigvals[:10]}")
    if args.do_permtest:
        print(perm_summary.strip())


if __name__ == "__main__":
    main()