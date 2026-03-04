// =============================================================================
// pipeline.cpp — Supply-Chain Lead-Lag Edge Construction Engine
// =============================================================================
// Combines 4 datasets into a normalised, weighted, directed edge list:
//   Customer ──(w_ji)──▶ Supplier
// where w_ji = salecs(i→j) / total_sale(i) (Revenue Dependency Metric).
//
// Compile: clang++ -std=c++17 -O2 -o pipeline pipeline.cpp
// Run:     ./pipeline
// =============================================================================

//add query1, query 3, file1 and file 2 from the previous two queries. put them all in the same folder.
//file structure should look like this
//query1/
//  yiay0c9gvy7mzn7n.csv
//  syaj3hxumfqnozvb.csv
//file1.csv
//file2.csv
//psci31kycwth7rnj.csv
//pipeline.cpp

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

// =============================================================================
// 0. Utility — Timing
// =============================================================================
struct Timer {
    using Clock = std::chrono::high_resolution_clock;
    Clock::time_point start;
    Timer() : start(Clock::now()) {}
    double elapsed_ms() const {
        return std::chrono::duration<double, std::milli>(Clock::now() - start).count();
    }
    void report(const std::string& label) const {
        std::cout << "  ⏱  " << label << ": "
                  << std::fixed << std::setprecision(1) << elapsed_ms() << " ms\n";
    }
};

// =============================================================================
// 1. String Normalisation & Tokenisation
// =============================================================================

// Strip non-alphanumeric chars, uppercase — used as the hash key for O(1) lookup.
static std::string normalise(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (unsigned char c : s) {
        if (std::isalnum(c)) out += static_cast<char>(std::toupper(c));
    }
    return out;
}

// Tokenise a normalised string into alphanumeric words (preserves digits like "3M", "7ELEVEN").
static std::vector<std::string> tokenise(const std::string& norm) {
    std::vector<std::string> tokens;
    std::string cur;
    for (char c : norm) {
        if (std::isalnum(static_cast<unsigned char>(c))) {
            cur += c;
        } else {
            if (!cur.empty()) { tokens.push_back(cur); cur.clear(); }
        }
    }
    if (!cur.empty()) tokens.push_back(cur);
    return tokens;
}

// Jaccard similarity on token sets — |A∩B| / |A∪B|.
static double jaccard(const std::vector<std::string>& a,
                      const std::vector<std::string>& b) {
    std::unordered_set<std::string> sa(a.begin(), a.end());
    std::unordered_set<std::string> sb(b.begin(), b.end());
    int inter = 0;
    for (auto& t : sa) inter += sb.count(t);
    int uni = static_cast<int>(sa.size() + sb.size()) - inter;
    return uni == 0 ? 0.0 : static_cast<double>(inter) / uni;
}

// Suffix stripping for common corporate suffixes that pollute matching.
static const std::vector<std::string> kCorpSuffixes = {
    "INC", "CORP", "CO", "LTD", "LLC", "LP", "PLC", "NV", "SA", "AG",
    "GROUP", "HOLDINGS", "ENTERPRISES", "INTERNATIONAL", "INTL",
    "COMPANY", "COMPANIES", "THE", "OF", "AND"
};

static std::string stripSuffixes(const std::string& norm) {
    auto tokens = tokenise(norm);
    std::vector<std::string> filtered;
    std::unordered_set<std::string> suf(kCorpSuffixes.begin(), kCorpSuffixes.end());
    for (auto& t : tokens) {
        if (suf.find(t) == suf.end()) filtered.push_back(t);
    }
    std::string out;
    for (auto& t : filtered) out += t;
    return out;
}

// =============================================================================
// 2. CSV Parsing (handles quoted fields)
// =============================================================================

static std::vector<std::string> splitCSV(const std::string& line) {
    std::vector<std::string> tokens;
    std::string token;
    bool inQ = false;
    for (char c : line) {
        if (c == '"')          { inQ = !inQ; }
        else if (c == ',' && !inQ) { tokens.push_back(token); token.clear(); }
        else                   { token += c; }
    }
    tokens.push_back(token);
    return tokens;
}

// Trim leading/trailing whitespace.
static std::string trim(const std::string& s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    if (a == std::string::npos) return "";
    size_t b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}

// =============================================================================
// 3. Data Structures
// =============================================================================

struct FirmMeta {
    std::string tic;        // clean ticker
    std::string conm;       // official company name
    std::string normName;   // normalised conm (hash key)
    std::string stripped;   // suffix-stripped normalised name (fallback key)
    std::vector<std::string> tokens; // tokenised for Jaccard
    bool active = false;    // costat == 'A'
};

struct FundamentalRow {
    std::string gvkey;
    std::string datadate;   // YYYY-MM-DD
    double sale = 0.0;      // total sales (denominator)
};

struct EarningsDate {
    std::string tic;
    std::string datadate;   // fiscal period end
    std::string rdq;        // report date of quarterly earnings
};

struct WeightedEdge {
    std::string date;              // srcdate (point-in-time)
    std::string customer_tic;      // Customer ticker (resolved)
    std::string supplier_tic;      // Supplier ticker
    std::string supplier_gvkey;    // Supplier GVKEY
    double salecs   = 0.0;        // numerator: sales(supplier→customer)
    double totalSale = 0.0;       // denominator: total sales of supplier
    double weight    = 0.0;       // w_ji = salecs / totalSale
    int    matchTier = 0;         // 1=exact, 2=stripped, 3=jaccard
};

// =============================================================================
// 4. Entity Resolution Engine
// =============================================================================
//
// Three-tier cascading lookup for dirty customer names → clean tickers:
//   Tier 1: Exact normalised match           — O(1) hash lookup
//   Tier 2: Suffix-stripped normalised match  — O(1) hash lookup
//   Tier 3: Jaccard token similarity ≥ θ      — O(|dictionary|) linear scan
//
// Tier 3 results are cached so subsequent identical queries are O(1).
// =============================================================================

class EntityResolver {
public:
    static constexpr double kJaccardThreshold = 0.60;

    // Ingest metadata rows. Call once after loading file2.
    void build(const std::vector<FirmMeta>& firms) {
        firms_ = &firms;
        for (size_t i = 0; i < firms.size(); ++i) {
            const auto& f = firms[i];
            // Tier 1: normalised name → index
            if (exactMap_.find(f.normName) == exactMap_.end())
                exactMap_[f.normName] = i;
            // Tier 2: suffix-stripped → index
            if (!f.stripped.empty() && strippedMap_.find(f.stripped) == strippedMap_.end())
                strippedMap_[f.stripped] = i;
        }
        std::cout << "  [EntityResolver] Tier-1 keys: " << exactMap_.size()
                  << " | Tier-2 keys: " << strippedMap_.size()
                  << " | Tier-3 corpus: " << firms.size() << "\n";
    }

    // Resolve a dirty customer name string → optional (ticker, matchTier).
    struct Result { std::string tic; int tier; };

    std::optional<Result> resolve(const std::string& dirtyName) {
        std::string norm = normalise(dirtyName);
        if (norm.empty()) return std::nullopt;

        // --- Check cache first (covers Tier 3 memoisation) ---
        auto cacheIt = cache_.find(norm);
        if (cacheIt != cache_.end()) return cacheIt->second;

        // --- Tier 1: exact normalised match ---
        auto it1 = exactMap_.find(norm);
        if (it1 != exactMap_.end()) {
            Result r{(*firms_)[it1->second].tic, 1};
            cache_[norm] = r;
            stats_.tier1++;
            return r;
        }

        // --- Tier 2: suffix-stripped match ---
        std::string stripped = stripSuffixes(norm);
        if (!stripped.empty()) {
            auto it2 = strippedMap_.find(stripped);
            if (it2 != strippedMap_.end()) {
                Result r{(*firms_)[it2->second].tic, 2};
                cache_[norm] = r;
                stats_.tier2++;
                return r;
            }
        }

        // --- Tier 3: Jaccard similarity (linear scan, cached) ---
        auto queryTokens = tokenise(norm);
        if (queryTokens.empty()) return std::nullopt;

        double bestScore = 0.0;
        size_t bestIdx = 0;
        for (size_t i = 0; i < firms_->size(); ++i) {
            double j = jaccard(queryTokens, (*firms_)[i].tokens);
            if (j > bestScore) { bestScore = j; bestIdx = i; }
        }
        if (bestScore >= kJaccardThreshold) {
            Result r{(*firms_)[bestIdx].tic, 3};
            cache_[norm] = r;
            stats_.tier3++;
            return r;
        }

        stats_.unresolved++;
        cache_[norm] = std::nullopt; // negative cache
        return std::nullopt;
    }

    struct Stats { int tier1 = 0, tier2 = 0, tier3 = 0, unresolved = 0; };
    const Stats& stats() const { return stats_; }

private:
    const std::vector<FirmMeta>* firms_ = nullptr;
    std::unordered_map<std::string, size_t> exactMap_;
    std::unordered_map<std::string, size_t> strippedMap_;
    std::unordered_map<std::string, std::optional<Result>> cache_;
    Stats stats_;
};

// =============================================================================
// 5. Macro / Non-Tradeable Entity Filter
// =============================================================================
// These are sink nodes with no ticker — government agencies, geographic
// segments, aggregated buckets.  We skip them before entity resolution.

static std::unordered_set<std::string> buildIgnoreSet() {
    const std::vector<std::string> raw = {
        "US Government", "U.S. Government", "U.S. Department of Defense",
        "Foreign Sales", "Not Reported", "Commercial",
        "Other Government and Defense", "Other Government",
        "Federal Government", "State and Local Government",
        "International", "Domestic", "Export", "Exports",
        "Intercompany", "Intersegment", "Eliminations",
        "Corporate and Other", "All Other", "Other",
        "Unallocated", "Non-US", "Asia Pacific", "Europe",
        "Americas", "United States", "Rest of World",
        "Middle East", "Latin America", "Japan", "China",
        "Canada", "UK", "Germany", "France", "India",
    };
    std::unordered_set<std::string> s;
    for (auto& r : raw) s.insert(normalise(r));
    return s;
}

// =============================================================================
// 6. Loader Functions
// =============================================================================

// --- File 2: Metadata (Node Dictionary) ---
static std::vector<FirmMeta> loadMetadata(const std::string& path) {
    Timer t;
    std::ifstream in(path);
    if (!in.is_open()) { std::cerr << "[!] Cannot open " << path << "\n"; return {}; }

    std::vector<FirmMeta> firms;
    firms.reserve(220000);
    std::string line;
    std::getline(in, line); // skip header: costat,curcd,datafmt,indfmt,consol,tic,datadate,gvkey,conm

    while (std::getline(in, line)) {
        auto row = splitCSV(line);
        if (row.size() < 9) continue;

        FirmMeta f;
        f.active   = (trim(row[0]) == "A");
        f.tic      = trim(row[5]);
        f.conm     = trim(row[8]);
        f.normName = normalise(f.conm);
        f.stripped = stripSuffixes(f.normName);
        f.tokens   = tokenise(f.normName);

        if (!f.tic.empty() && !f.normName.empty())
            firms.push_back(std::move(f));
    }
    // De-duplicate: keep first occurrence per normName (stable ordering preserves
    // active tickers that appear first in Compustat).
    std::unordered_set<std::string> seen;
    std::vector<FirmMeta> deduped;
    deduped.reserve(firms.size());
    for (auto& f : firms) {
        if (seen.insert(f.normName).second)
            deduped.push_back(std::move(f));
    }
    t.report("loadMetadata (" + std::to_string(deduped.size()) + " unique firms)");
    return deduped;
}

// --- File 3: Fundamentals (Total Sales — the denominator) ---
// Key: gvkey + "_" + datadate  →  total sale
static std::unordered_map<std::string, double>
loadFundamentals(const std::string& path) {
    Timer t;
    std::ifstream in(path);
    if (!in.is_open()) { std::cerr << "[!] Cannot open " << path << "\n"; return {}; }

    // Header: costat,curcd,datafmt,indfmt,consol,tic,datadate,gvkey,conm,fyr,gsector,at,sale,sich
    //         0      1     2       3      4      5   6        7     8    9   10      11 12   13
    std::unordered_map<std::string, double> m;
    m.reserve(250000);
    std::string line;
    std::getline(in, line); // skip header

    while (std::getline(in, line)) {
        auto row = splitCSV(line);
        if (row.size() < 13) continue;

        std::string gvkey    = trim(row[7]);
        std::string datadate = trim(row[6]);
        std::string saleStr  = trim(row[12]);

        if (saleStr.empty()) continue;
        try {
            double sale = std::stod(saleStr);
            if (sale > 0.0) {
                m[gvkey + "_" + datadate] = sale;
            }
        } catch (...) {}
    }
    t.report("loadFundamentals (" + std::to_string(m.size()) + " rows)");
    return m;
}

// --- File 4: Earnings Dates (Shock Timestamps) ---
// Key: tic + "_" + datadate → rdq (report date of quarterly earnings)
static std::unordered_map<std::string, std::string>
loadEarningsDates(const std::string& path) {
    Timer t;
    std::ifstream in(path);
    if (!in.is_open()) { std::cerr << "[!] Cannot open " << path << "\n"; return {}; }

    // Header: costat,curcdq,datafmt,indfmt,consol,tic,datadate,gvkey,rdq
    //         0      1      2       3      4      5   6        7     8
    std::unordered_map<std::string, std::string> m;
    m.reserve(800000);
    std::string line;
    std::getline(in, line); // skip header

    while (std::getline(in, line)) {
        auto row = splitCSV(line);
        if (row.size() < 9) continue;

        std::string tic     = trim(row[5]);
        std::string datadate = trim(row[6]);
        std::string rdq      = trim(row[8]);

        if (!tic.empty() && !rdq.empty())
            m[tic + "_" + datadate] = rdq;
    }
    t.report("loadEarningsDates (" + std::to_string(m.size()) + " rows)");
    return m;
}

// =============================================================================
// 7. Edge Processing Pipeline
// =============================================================================

struct PipelineStats {
    int64_t totalRows     = 0;
    int64_t skippedIgnore = 0;
    int64_t skippedNoSale = 0;
    int64_t skippedNoFund = 0;
    int64_t unresolved    = 0;
    int64_t validEdges    = 0;
    int64_t selfLoops     = 0;
    double  weightSum     = 0.0;
    double  maxWeight     = 0.0;

    void print() const {
        std::cout << "\n══════════════════════════════════════════════\n"
                  << "  Pipeline Statistics\n"
                  << "══════════════════════════════════════════════\n"
                  << "  Total segment rows read:  " << totalRows     << "\n"
                  << "  Skipped (macro entity):   " << skippedIgnore << "\n"
                  << "  Skipped (no salecs):      " << skippedNoSale << "\n"
                  << "  Skipped (no fundamental): " << skippedNoFund << "\n"
                  << "  Skipped (self-loops):     " << selfLoops     << "\n"
                  << "  Unresolved customers:     " << unresolved    << "\n"
                  << "  ✓ Valid weighted edges:   " << validEdges    << "\n"
                  << "  Mean weight w_ji:         " << std::fixed << std::setprecision(4)
                        << (validEdges > 0 ? weightSum / validEdges : 0.0) << "\n"
                  << "  Max  weight w_ji:         " << maxWeight     << "\n"
                  << "══════════════════════════════════════════════\n";
    }
};

static std::vector<WeightedEdge> processEdges(
        const std::string& file1Path,
        EntityResolver& resolver,
        const std::unordered_map<std::string, double>& fundMap,
        const std::unordered_set<std::string>& ignoreSet,
        PipelineStats& stats)
{
    Timer t;
    std::ifstream in(file1Path);
    if (!in.is_open()) { std::cerr << "[!] Cannot open " << file1Path << "\n"; return {}; }

    // Header: tic,srcdate,gvkey,conm,cid,cnms,salecs,sid
    //         0   1       2     3    4   5    6      7
    std::string line;
    std::getline(in, line); // skip header

    std::vector<WeightedEdge> edges;
    edges.reserve(200000);

    while (std::getline(in, line)) {
        auto row = splitCSV(line);
        if (row.size() < 7) continue;
        stats.totalRows++;

        std::string supplierTic  = trim(row[0]);
        std::string srcdate      = trim(row[1]);
        std::string gvkey        = trim(row[2]);
        std::string dirtyCustomer = trim(row[5]);
        std::string salecsStr    = trim(row[6]);

        // --- Filter: ignore macro/non-tradeable entities ---
        std::string normCust = normalise(dirtyCustomer);
        if (ignoreSet.count(normCust)) { stats.skippedIgnore++; continue; }

        // --- Filter: missing salecs ---
        if (salecsStr.empty()) { stats.skippedNoSale++; continue; }
        double salecs = 0.0;
        try { salecs = std::stod(salecsStr); } catch (...) { stats.skippedNoSale++; continue; }
        if (salecs <= 0.0) { stats.skippedNoSale++; continue; }

        // --- Entity Resolution: dirty name → ticker ---
        auto resolved = resolver.resolve(dirtyCustomer);
        if (!resolved) { stats.unresolved++; continue; }
        std::string customerTic = resolved->tic;

        // --- Filter: self-loops (supplier == customer) ---
        if (customerTic == supplierTic) { stats.selfLoops++; continue; }

        // --- Fetch denominator (total sales) ---
        std::string fundKey = gvkey + "_" + srcdate;
        auto fundIt = fundMap.find(fundKey);
        if (fundIt == fundMap.end()) { stats.skippedNoFund++; continue; }
        double totalSale = fundIt->second;
        if (totalSale <= 0.0) { stats.skippedNoFund++; continue; }

        // --- Compute weight w_ji = salecs / totalSale, capped at 1.0 ---
        double w = salecs / totalSale;
        if (w > 1.0) w = 1.0; // data anomaly cap

        WeightedEdge e;
        e.date          = srcdate;
        e.customer_tic  = customerTic;
        e.supplier_tic  = supplierTic;
        e.supplier_gvkey = gvkey;
        e.salecs        = salecs;
        e.totalSale     = totalSale;
        e.weight        = w;
        e.matchTier     = resolved->tier;
        edges.push_back(std::move(e));

        stats.validEdges++;
        stats.weightSum += w;
        if (w > stats.maxWeight) stats.maxWeight = w;
    }
    t.report("processEdges (" + std::to_string(stats.validEdges) + " valid)");
    return edges;
}

// =============================================================================
// 8. Output Writers
// =============================================================================

// --- 8a. Weighted Edge List (primary output) ---
static void writeEdgeList(const std::string& path,
                          const std::vector<WeightedEdge>& edges) {
    Timer t;
    std::ofstream out(path);
    out << "date,customer_tic,supplier_tic,supplier_gvkey,"
           "salecs,total_sale,weight_wji,match_tier\n";
    out << std::fixed << std::setprecision(6);
    for (auto& e : edges) {
        out << e.date          << ','
            << e.customer_tic  << ','
            << e.supplier_tic  << ','
            << e.supplier_gvkey << ','
            << e.salecs        << ','
            << e.totalSale     << ','
            << e.weight        << ','
            << e.matchTier     << '\n';
    }
    t.report("writeEdgeList → " + path);
}

// --- 8b. Adjacency List (per-date snapshot, for Graph Laplacian construction) ---
// Format: date | supplier_tic | [customer_tic:weight, ...]
static void writeAdjacencyList(const std::string& path,
                               const std::vector<WeightedEdge>& edges) {
    Timer t;
    // Group edges by (date, supplier_tic)
    struct AdjKey {
        std::string date;
        std::string supplier;
        bool operator==(const AdjKey& o) const { return date == o.date && supplier == o.supplier; }
    };
    struct AdjHash {
        size_t operator()(const AdjKey& k) const {
            size_t h1 = std::hash<std::string>{}(k.date);
            size_t h2 = std::hash<std::string>{}(k.supplier);
            return h1 ^ (h2 << 1);
        }
    };

    std::unordered_map<AdjKey, std::vector<std::pair<std::string, double>>, AdjHash> adj;
    for (auto& e : edges) {
        adj[{e.date, e.supplier_tic}].push_back({e.customer_tic, e.weight});
    }

    std::ofstream out(path);
    out << "date,supplier_tic,neighbors\n";
    out << std::fixed << std::setprecision(6);
    for (auto& [key, neighbors] : adj) {
        out << key.date << ',' << key.supplier << ',';
        for (size_t i = 0; i < neighbors.size(); ++i) {
            if (i > 0) out << '|';
            out << neighbors[i].first << ':' << neighbors[i].second;
        }
        out << '\n';
    }
    t.report("writeAdjacencyList → " + path);
}

// --- 8c. Earnings Calendar (enriched with customer tickers for event studies) ---
static void writeEarningsCalendar(
        const std::string& path,
        const std::vector<WeightedEdge>& edges,
        const std::unordered_map<std::string, std::string>& earningsMap) {
    Timer t;
    // Collect unique customer tickers from the edge set
    std::unordered_set<std::string> customerTics;
    for (auto& e : edges) customerTics.insert(e.customer_tic);

    std::ofstream out(path);
    out << "tic,fiscal_period_end,report_date_rdq\n";

    int written = 0;
    for (auto& [key, rdq] : earningsMap) {
        // key = tic_datadate
        auto pos = key.find('_');
        if (pos == std::string::npos) continue;
        std::string tic = key.substr(0, pos);
        std::string datadate = key.substr(pos + 1);

        // Only output earnings for firms that appear as customers in our graph
        if (customerTics.count(tic)) {
            out << tic << ',' << datadate << ',' << rdq << '\n';
            written++;
        }
    }
    t.report("writeEarningsCalendar (" + std::to_string(written) + " events) → " + path);
}

// =============================================================================
// 9. Main
// =============================================================================

int main() {
    std::cout << "══════════════════════════════════════════════\n"
              << "  Supply-Chain Lead-Lag Pipeline\n"
              << "══════════════════════════════════════════════\n\n";
    Timer total;

    // ----- File Paths -----
    const std::string kFile1 = "query1/file1.csv";                     // segments (raw edges)
    const std::string kFile2 = "query1/file2.csv";                     // metadata  (node dict)
    const std::string kFile3 = "query1/yiay0c9gvy7mzn7n.csv";         // fundamentals (denom)
    const std::string kFile4 = "query1/syaj3hxumfqnozvb.csv";         // earnings dates

    const std::string kOutEdges    = "output_edges.csv";
    const std::string kOutAdj      = "output_adjacency.csv";
    const std::string kOutEarnings = "output_earnings_calendar.csv";

    // ===== Stage 1: Load Metadata & Build Entity Resolver =====
    std::cout << "[Stage 1] Loading metadata & building entity resolver...\n";
    auto firms = loadMetadata(kFile2);
    if (firms.empty()) { std::cerr << "[FATAL] No metadata loaded.\n"; return 1; }

    EntityResolver resolver;
    resolver.build(firms);

    // ===== Stage 2: Load Fundamentals (Denominator) =====
    std::cout << "\n[Stage 2] Loading fundamentals (total sales)...\n";
    auto fundMap = loadFundamentals(kFile3);

    // ===== Stage 3: Load Earnings Dates =====
    std::cout << "\n[Stage 3] Loading earnings dates...\n";
    auto earningsMap = loadEarningsDates(kFile4);

    // ===== Stage 4: Process Edges =====
    std::cout << "\n[Stage 4] Processing supply-chain edges...\n";
    auto ignoreSet = buildIgnoreSet();
    PipelineStats pstats;
    auto edges = processEdges(kFile1, resolver, fundMap, ignoreSet, pstats);

    // ===== Stage 5: Write Outputs =====
    std::cout << "\n[Stage 5] Writing outputs...\n";
    writeEdgeList(kOutEdges, edges);
    writeAdjacencyList(kOutAdj, edges);
    writeEarningsCalendar(kOutEarnings, edges, earningsMap);

    // ===== Stage 6: Report =====
    pstats.print();

    auto& rs = resolver.stats();
    std::cout << "\n  Entity Resolution Breakdown:\n"
              << "    Tier 1 (exact):    " << rs.tier1      << "\n"
              << "    Tier 2 (stripped): " << rs.tier2      << "\n"
              << "    Tier 3 (Jaccard):  " << rs.tier3      << "\n"
              << "    Unresolved:        " << rs.unresolved << "\n";

    // Unique nodes / edges summary
    std::unordered_set<std::string> uniqueCustomers, uniqueSuppliers;
    for (auto& e : edges) {
        uniqueCustomers.insert(e.customer_tic);
        uniqueSuppliers.insert(e.supplier_tic);
    }
    std::cout << "\n  Graph Summary:\n"
              << "    Unique customer nodes: " << uniqueCustomers.size() << "\n"
              << "    Unique supplier nodes: " << uniqueSuppliers.size() << "\n"
              << "    Directed edges:        " << edges.size()           << "\n";

    total.report("\n  Total pipeline runtime");
    std::cout << "\n[✓] Done. Outputs written to:\n"
              << "    " << kOutEdges    << "\n"
              << "    " << kOutAdj      << "\n"
              << "    " << kOutEarnings << "\n";

    return 0;
}
