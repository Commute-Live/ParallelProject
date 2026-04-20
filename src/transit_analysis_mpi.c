#include "clockcycle.h"
#include "transit_analysis.h"

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <math.h>
#include <mpi.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef enum {
    BACKEND_CPU = 0,
    BACKEND_CUDA = 1
} backend_t;

typedef struct {
    const char *input_path;
    const char *city_report_path;
    const char *bucket_report_path;
    backend_t backend;
    uint64_t requested_records;
    uint32_t bucket_width_seconds;
    int emit_csv;
} options_t;

typedef struct {
    int valid;
    trip_record_t record;
} boundary_record_t;

typedef struct {
    uint64_t records_analyzed;
    uint32_t city_count;
    double io_seconds;
    double boundary_seconds;
    double compute_seconds;
    double reduce_seconds;
    double total_seconds;
    double mean_peak_share;
    double mean_headway_seconds;
    double mean_headway_cv;
} run_metrics_t;

static void print_usage(const char *program)
{
    fprintf(stderr,
            "Usage: %s --input <trip_records.bin> [--backend cpu|cuda] "
            "[--records <count>] [--bucket-width-seconds <seconds>] "
            "[--city-report <path>] [--bucket-report <path>] [--csv]\n",
            program);
}

static void *xcalloc(size_t count, size_t size)
{
    void *ptr = calloc(count, size);
    if (ptr == NULL) {
        fprintf(stderr, "calloc failed for %zu bytes\n", count * size);
        exit(1);
    }
    return ptr;
}

static int parse_u64(const char *text, uint64_t *value)
{
    char *end = NULL;
    unsigned long long parsed;

    errno = 0;
    parsed = strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0') {
        return 0;
    }

    *value = (uint64_t)parsed;
    return 1;
}

static int parse_u32(const char *text, uint32_t *value)
{
    uint64_t parsed = 0;
    if (!parse_u64(text, &parsed) || parsed > UINT32_MAX) {
        return 0;
    }

    *value = (uint32_t)parsed;
    return 1;
}

static int parse_options(int argc, char **argv, options_t *options)
{
    int i;

    memset(options, 0, sizeof(*options));
    options->backend = BACKEND_CUDA;
    options->bucket_width_seconds = TRANSIT_BUCKET_WIDTH_SECONDS;

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--input") == 0 && i + 1 < argc) {
            options->input_path = argv[++i];
        } else if (strcmp(argv[i], "--backend") == 0 && i + 1 < argc) {
            const char *backend = argv[++i];
            if (strcmp(backend, "cpu") == 0) {
                options->backend = BACKEND_CPU;
            } else if (strcmp(backend, "cuda") == 0) {
                options->backend = BACKEND_CUDA;
            } else {
                return 0;
            }
        } else if (strcmp(argv[i], "--records") == 0 && i + 1 < argc) {
            if (!parse_u64(argv[++i], &options->requested_records)) {
                return 0;
            }
        } else if (strcmp(argv[i], "--bucket-width-seconds") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], &options->bucket_width_seconds) ||
                options->bucket_width_seconds == 0 ||
                86400U % options->bucket_width_seconds != 0U) {
                return 0;
            }
        } else if (strcmp(argv[i], "--city-report") == 0 && i + 1 < argc) {
            options->city_report_path = argv[++i];
        } else if (strcmp(argv[i], "--bucket-report") == 0 && i + 1 < argc) {
            options->bucket_report_path = argv[++i];
        } else if (strcmp(argv[i], "--csv") == 0) {
            options->emit_csv = 1;
        } else {
            return 0;
        }
    }

    return options->input_path != NULL;
}

int transit_is_peak_window(uint32_t normalized_start_secs)
{
    return ((normalized_start_secs >= TRANSIT_PEAK_AM_START &&
             normalized_start_secs < TRANSIT_PEAK_AM_END) ||
            (normalized_start_secs >= TRANSIT_PEAK_PM_START &&
             normalized_start_secs < TRANSIT_PEAK_PM_END));
}

int transit_same_headway_group(const trip_record_t *lhs, const trip_record_t *rhs)
{
    return lhs->city_id == rhs->city_id &&
           lhs->route_hash == rhs->route_hash &&
           lhs->direction_id == rhs->direction_id;
}

static void stats_zero(analysis_stats_t *stats)
{
    memset(stats, 0, sizeof(*stats));
}

static void stats_alloc(analysis_stats_t *stats,
                        uint32_t city_count,
                        uint32_t bucket_count,
                        uint32_t route_type_bins)
{
    size_t city_slots = (size_t)city_count;
    size_t bucket_slots = city_slots * (size_t)bucket_count;
    size_t route_slots = city_slots * (size_t)route_type_bins;

    stats_zero(stats);
    stats->departures_per_bucket = (uint64_t *)xcalloc(bucket_slots, sizeof(uint64_t));
    stats->route_type_counts = (uint64_t *)xcalloc(route_slots, sizeof(uint64_t));
    stats->total_departures = (uint64_t *)xcalloc(city_slots, sizeof(uint64_t));
    stats->peak_departures = (uint64_t *)xcalloc(city_slots, sizeof(uint64_t));
    stats->offpeak_departures = (uint64_t *)xcalloc(city_slots, sizeof(uint64_t));
    stats->headway_count = (uint64_t *)xcalloc(city_slots, sizeof(uint64_t));
    stats->bunch_count = (uint64_t *)xcalloc(city_slots, sizeof(uint64_t));
    stats->wide_gap_count = (uint64_t *)xcalloc(city_slots, sizeof(uint64_t));
    stats->trip_count = (uint64_t *)xcalloc(city_slots, sizeof(uint64_t));
    stats->stop_count_sum = (uint64_t *)xcalloc(city_slots, sizeof(uint64_t));
    stats->trip_duration_sum = (double *)xcalloc(city_slots, sizeof(double));
    stats->trip_duration_sq_sum = (double *)xcalloc(city_slots, sizeof(double));
    stats->headway_sum = (double *)xcalloc(city_slots, sizeof(double));
    stats->headway_sq_sum = (double *)xcalloc(city_slots, sizeof(double));
}

static void stats_free(analysis_stats_t *stats)
{
    free(stats->departures_per_bucket);
    free(stats->route_type_counts);
    free(stats->total_departures);
    free(stats->peak_departures);
    free(stats->offpeak_departures);
    free(stats->headway_count);
    free(stats->bunch_count);
    free(stats->wide_gap_count);
    free(stats->trip_count);
    free(stats->stop_count_sum);
    free(stats->trip_duration_sum);
    free(stats->trip_duration_sq_sum);
    free(stats->headway_sum);
    free(stats->headway_sq_sum);
    stats_zero(stats);
}

static void report_mpi_error(int rank, const char *context, int code)
{
    char error_text[MPI_MAX_ERROR_STRING];
    int length = 0;

    MPI_Error_string(code, error_text, &length);
    fprintf(stderr, "Rank %d: %s failed: %.*s\n", rank, context, length, error_text);
}

static void compute_partition(uint64_t total_records,
                              int nranks,
                              int rank,
                              uint64_t *local_offset,
                              uint64_t *local_count)
{
    uint64_t base = total_records / (uint64_t)nranks;
    uint64_t extra = total_records % (uint64_t)nranks;
    uint64_t count = base + ((uint64_t)rank < extra ? 1ULL : 0ULL);
    uint64_t offset = base * (uint64_t)rank +
                      ((uint64_t)rank < extra ? (uint64_t)rank : extra);

    *local_offset = offset;
    *local_count = count;
}

static int select_gpu(MPI_Comm comm)
{
    MPI_Comm shared_comm = MPI_COMM_NULL;
    int local_rank = 0;
    int device_count = 0;

    MPI_Comm_split_type(comm, MPI_COMM_TYPE_SHARED, 0, MPI_INFO_NULL, &shared_comm);
    MPI_Comm_rank(shared_comm, &local_rank);
    MPI_Comm_free(&shared_comm);

    device_count = transit_cuda_get_device_count();
    if (device_count <= 0) {
        return -1;
    }

    return local_rank % device_count;
}

static uint32_t normalize_to_day(uint32_t seconds)
{
    return seconds % 86400U;
}

void transit_analyze_cpu(const trip_record_t *records,
                         size_t record_count,
                         const trip_record_t *previous_record,
                         int has_previous_record,
                         uint32_t city_count,
                         uint32_t bucket_count,
                         uint32_t route_type_bins,
                         analysis_stats_t *stats)
{
    size_t i;
    trip_record_t prior = {0};
    int has_prior = 0;
    uint32_t bucket_width = 86400U / bucket_count;

    if (has_previous_record) {
        prior = *previous_record;
        has_prior = 1;
    }

    for (i = 0; i < record_count; i++) {
        const trip_record_t *record = &records[i];
        uint32_t city_id = record->city_id;
        uint32_t route_type = record->route_type < route_type_bins
                                  ? record->route_type
                                  : (route_type_bins - 1U);
        uint32_t normalized_start = normalize_to_day(record->start_secs);
        uint32_t bucket = normalized_start / bucket_width;
        double duration = (double)record->duration_secs;

        if (bucket >= bucket_count) {
            bucket = bucket_count - 1U;
        }

        stats->departures_per_bucket[(size_t)city_id * bucket_count + bucket] += 1ULL;
        stats->route_type_counts[(size_t)city_id * route_type_bins + route_type] += 1ULL;
        stats->total_departures[city_id] += 1ULL;
        stats->trip_count[city_id] += 1ULL;
        stats->stop_count_sum[city_id] += (uint64_t)record->stop_count;
        stats->trip_duration_sum[city_id] += duration;
        stats->trip_duration_sq_sum[city_id] += duration * duration;

        if (transit_is_peak_window(normalized_start)) {
            stats->peak_departures[city_id] += 1ULL;
        } else {
            stats->offpeak_departures[city_id] += 1ULL;
        }

        if (has_prior && transit_same_headway_group(&prior, record) &&
            record->start_secs >= prior.start_secs) {
            uint32_t gap = record->start_secs - prior.start_secs;
            double gap_d = (double)gap;

            stats->headway_count[city_id] += 1ULL;
            stats->headway_sum[city_id] += gap_d;
            stats->headway_sq_sum[city_id] += gap_d * gap_d;
            if (gap < TRANSIT_BUNCH_THRESHOLD_SECONDS) {
                stats->bunch_count[city_id] += 1ULL;
            }
            if (gap > TRANSIT_WIDE_GAP_THRESHOLD_SECONDS) {
                stats->wide_gap_count[city_id] += 1ULL;
            }
        }

        prior = *record;
        has_prior = 1;
    }

    (void)city_count;
}

static run_metrics_t summarize_metrics(const analysis_stats_t *stats,
                                       uint32_t city_count,
                                       uint64_t records_analyzed,
                                       double io_seconds,
                                       double boundary_seconds,
                                       double compute_seconds,
                                       double reduce_seconds,
                                       double total_seconds)
{
    run_metrics_t result;
    uint32_t city_id;
    double peak_share_sum = 0.0;
    double headway_mean_sum = 0.0;
    double headway_cv_sum = 0.0;
    uint32_t peak_share_cities = 0U;
    uint32_t headway_cities = 0U;

    memset(&result, 0, sizeof(result));
    result.records_analyzed = records_analyzed;
    result.city_count = city_count;
    result.io_seconds = io_seconds;
    result.boundary_seconds = boundary_seconds;
    result.compute_seconds = compute_seconds;
    result.reduce_seconds = reduce_seconds;
    result.total_seconds = total_seconds;

    for (city_id = 0; city_id < city_count; city_id++) {
        if (stats->total_departures[city_id] > 0ULL) {
            peak_share_sum += (double)stats->peak_departures[city_id] /
                              (double)stats->total_departures[city_id];
            peak_share_cities++;
        }
        if (stats->headway_count[city_id] > 0ULL) {
            double mean = stats->headway_sum[city_id] / (double)stats->headway_count[city_id];
            double mean_sq = stats->headway_sq_sum[city_id] /
                             (double)stats->headway_count[city_id];
            double variance = mean_sq - mean * mean;
            if (variance < 0.0) {
                variance = 0.0;
            }
            headway_mean_sum += mean;
            headway_cv_sum += (mean > 0.0) ? sqrt(variance) / mean : 0.0;
            headway_cities++;
        }
    }

    result.mean_peak_share = peak_share_cities > 0U
                                 ? peak_share_sum / (double)peak_share_cities
                                 : 0.0;
    result.mean_headway_seconds = headway_cities > 0U
                                      ? headway_mean_sum / (double)headway_cities
                                      : 0.0;
    result.mean_headway_cv = headway_cities > 0U
                                 ? headway_cv_sum / (double)headway_cities
                                 : 0.0;

    return result;
}

static void write_city_report(const char *path,
                              const analysis_stats_t *stats,
                              uint32_t city_count)
{
    FILE *handle = fopen(path, "w");
    uint32_t city_id;

    if (handle == NULL) {
        fprintf(stderr, "Unable to open city report %s\n", path);
        return;
    }

    fprintf(handle,
            "city_id,total_departures,peak_departures,offpeak_departures,"
            "peak_share,avg_trip_duration_sec,avg_headway_sec,headway_cv,"
            "bunch_rate,wide_gap_rate,avg_stop_count\n");

    for (city_id = 0; city_id < city_count; city_id++) {
        double peak_share = 0.0;
        double avg_trip_duration = 0.0;
        double avg_headway = 0.0;
        double headway_cv = 0.0;
        double bunch_rate = 0.0;
        double wide_gap_rate = 0.0;
        double avg_stop_count = 0.0;

        if (stats->total_departures[city_id] > 0ULL) {
            peak_share = (double)stats->peak_departures[city_id] /
                         (double)stats->total_departures[city_id];
        }
        if (stats->trip_count[city_id] > 0ULL) {
            avg_trip_duration = stats->trip_duration_sum[city_id] /
                                (double)stats->trip_count[city_id];
            avg_stop_count = (double)stats->stop_count_sum[city_id] /
                             (double)stats->trip_count[city_id];
        }
        if (stats->headway_count[city_id] > 0ULL) {
            double mean = stats->headway_sum[city_id] / (double)stats->headway_count[city_id];
            double mean_sq = stats->headway_sq_sum[city_id] /
                             (double)stats->headway_count[city_id];
            double variance = mean_sq - mean * mean;
            if (variance < 0.0) {
                variance = 0.0;
            }
            avg_headway = mean;
            headway_cv = mean > 0.0 ? sqrt(variance) / mean : 0.0;
            bunch_rate = (double)stats->bunch_count[city_id] /
                         (double)stats->headway_count[city_id];
            wide_gap_rate = (double)stats->wide_gap_count[city_id] /
                            (double)stats->headway_count[city_id];
        }

        fprintf(handle,
                "%" PRIu32 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%.6f,%.6f,%.6f,"
                "%.6f,%.6f,%.6f,%.6f\n",
                city_id,
                stats->total_departures[city_id],
                stats->peak_departures[city_id],
                stats->offpeak_departures[city_id],
                peak_share,
                avg_trip_duration,
                avg_headway,
                headway_cv,
                bunch_rate,
                wide_gap_rate,
                avg_stop_count);
    }

    fclose(handle);
}

static void write_bucket_report(const char *path,
                                const analysis_stats_t *stats,
                                uint32_t city_count,
                                uint32_t bucket_count,
                                uint32_t bucket_width_seconds)
{
    FILE *handle = fopen(path, "w");
    uint32_t city_id;
    uint32_t bucket_id;

    if (handle == NULL) {
        fprintf(stderr, "Unable to open bucket report %s\n", path);
        return;
    }

    fprintf(handle, "city_id,bucket_id,bucket_start_seconds,departures\n");
    for (city_id = 0; city_id < city_count; city_id++) {
        for (bucket_id = 0; bucket_id < bucket_count; bucket_id++) {
            fprintf(handle,
                    "%" PRIu32 ",%" PRIu32 ",%" PRIu32 ",%" PRIu64 "\n",
                    city_id,
                    bucket_id,
                    bucket_id * bucket_width_seconds,
                    stats->departures_per_bucket[(size_t)city_id * bucket_count + bucket_id]);
        }
    }

    fclose(handle);
}

static int read_trip_records(MPI_Comm comm,
                             const char *path,
                             uint64_t requested_records,
                             trip_record_t **records_out,
                             uint64_t *total_records_out,
                             uint64_t *local_offset_out,
                             uint64_t *local_count_out,
                             double *io_seconds_out)
{
    MPI_File file = MPI_FILE_NULL;
    MPI_Datatype record_type = MPI_DATATYPE_NULL;
    MPI_Offset file_size = 0;
    uint64_t total_records = 0;
    uint64_t local_offset = 0;
    uint64_t local_count = 0;
    trip_record_t *records = NULL;
    uint64_t start_ticks;
    uint64_t end_ticks;
    int rank = 0;
    int nranks = 0;
    int rc;

    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &nranks);

    rc = MPI_File_open(comm, (char *)path, MPI_MODE_RDONLY, MPI_INFO_NULL, &file);
    if (rc != MPI_SUCCESS) {
        report_mpi_error(rank, "MPI_File_open", rc);
        return 0;
    }

    rc = MPI_File_get_size(file, &file_size);
    if (rc != MPI_SUCCESS) {
        report_mpi_error(rank, "MPI_File_get_size", rc);
        MPI_File_close(&file);
        return 0;
    }

    if ((uint64_t)file_size % (uint64_t)sizeof(trip_record_t) != 0ULL) {
        if (rank == 0) {
            fprintf(stderr,
                    "Input file size %" PRIu64 " is not a multiple of trip record size %zu\n",
                    (uint64_t)file_size,
                    sizeof(trip_record_t));
        }
        MPI_File_close(&file);
        return 0;
    }

    total_records = (uint64_t)file_size / (uint64_t)sizeof(trip_record_t);
    if (requested_records > 0ULL && requested_records < total_records) {
        total_records = requested_records;
    }

    compute_partition(total_records, nranks, rank, &local_offset, &local_count);

    if (local_count > 0ULL) {
        if (local_count > (uint64_t)INT_MAX) {
            fprintf(stderr, "Rank %d local_count exceeds MPI int count limit\n", rank);
            MPI_File_close(&file);
            return 0;
        }

        records = (trip_record_t *)xcalloc((size_t)local_count, sizeof(trip_record_t));
    }

    MPI_Type_contiguous((int)sizeof(trip_record_t), MPI_BYTE, &record_type);
    MPI_Type_commit(&record_type);

    MPI_Barrier(comm);
    start_ticks = clock_now();

    rc = MPI_File_read_at_all(file,
                              (MPI_Offset)(local_offset * (uint64_t)sizeof(trip_record_t)),
                              records,
                              (int)local_count,
                              record_type,
                              MPI_STATUS_IGNORE);

    end_ticks = clock_now();
    if (rc != MPI_SUCCESS) {
        report_mpi_error(rank, "MPI_File_read_at_all", rc);
        MPI_Type_free(&record_type);
        MPI_File_close(&file);
        free(records);
        return 0;
    }

    MPI_Type_free(&record_type);
    MPI_File_close(&file);

    *records_out = records;
    *total_records_out = total_records;
    *local_offset_out = local_offset;
    *local_count_out = local_count;
    *io_seconds_out = clock_ticks_to_seconds(end_ticks - start_ticks);
    return 1;
}

static boundary_record_t exchange_previous_record(MPI_Comm comm,
                                                  const trip_record_t *records,
                                                  uint64_t local_count,
                                                  double *boundary_seconds_out)
{
    boundary_record_t outgoing;
    boundary_record_t incoming;
    int rank = 0;
    int nranks = 0;
    int send_to;
    int recv_from;
    uint64_t start_ticks;
    uint64_t end_ticks;

    memset(&outgoing, 0, sizeof(outgoing));
    memset(&incoming, 0, sizeof(incoming));

    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &nranks);

    if (local_count > 0ULL) {
        outgoing.valid = 1;
        outgoing.record = records[local_count - 1ULL];
    }

    send_to = (rank + 1 < nranks) ? rank + 1 : MPI_PROC_NULL;
    recv_from = (rank - 1 >= 0) ? rank - 1 : MPI_PROC_NULL;

    MPI_Barrier(comm);
    start_ticks = clock_now();
    MPI_Sendrecv(&outgoing,
                 (int)sizeof(outgoing),
                 MPI_BYTE,
                 send_to,
                 77,
                 &incoming,
                 (int)sizeof(incoming),
                 MPI_BYTE,
                 recv_from,
                 77,
                 comm,
                 MPI_STATUS_IGNORE);
    end_ticks = clock_now();

    *boundary_seconds_out = clock_ticks_to_seconds(end_ticks - start_ticks);
    return incoming;
}

static void reduce_u64_array(MPI_Comm comm,
                             const uint64_t *local,
                             uint64_t *global,
                             int count)
{
    MPI_Reduce((void *)local, global, count, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, comm);
}

static void reduce_double_array(MPI_Comm comm,
                                const double *local,
                                double *global,
                                int count)
{
    MPI_Reduce((void *)local, global, count, MPI_DOUBLE, MPI_SUM, 0, comm);
}

static void print_summary(int rank,
                          int nranks,
                          const options_t *options,
                          const run_metrics_t *metrics,
                          const analysis_stats_t *global_stats)
{
    uint32_t city_id;

    if (rank != 0) {
        return;
    }

    printf("Backend: %s\n", options->backend == BACKEND_CUDA ? "cuda" : "cpu");
    printf("MPI Ranks: %d\n", nranks);
    printf("Records analyzed: %" PRIu64 "\n", metrics->records_analyzed);
    printf("Cities discovered: %" PRIu32 "\n", metrics->city_count);
    printf("Parallel I/O time: %.6f seconds\n", metrics->io_seconds);
    printf("Boundary exchange time: %.6f seconds\n", metrics->boundary_seconds);
    printf("Local compute time: %.6f seconds\n", metrics->compute_seconds);
    printf("Global reduction time: %.6f seconds\n", metrics->reduce_seconds);
    printf("Total analysis time: %.6f seconds\n", metrics->total_seconds);
    printf("Mean peak-share across cities: %.6f\n", metrics->mean_peak_share);
    printf("Mean headway across cities: %.6f seconds\n", metrics->mean_headway_seconds);
    printf("Mean headway CV across cities: %.6f\n", metrics->mean_headway_cv);

    for (city_id = 0; city_id < metrics->city_count; city_id++) {
        double avg_duration = 0.0;
        double avg_headway = 0.0;
        double avg_stop_count = 0.0;
        double headway_cv = 0.0;
        double peak_share = 0.0;

        if (global_stats->trip_count[city_id] > 0ULL) {
            avg_duration = global_stats->trip_duration_sum[city_id] /
                           (double)global_stats->trip_count[city_id];
            avg_stop_count = (double)global_stats->stop_count_sum[city_id] /
                             (double)global_stats->trip_count[city_id];
            peak_share = (double)global_stats->peak_departures[city_id] /
                         (double)global_stats->total_departures[city_id];
        }
        if (global_stats->headway_count[city_id] > 0ULL) {
            double mean = global_stats->headway_sum[city_id] /
                          (double)global_stats->headway_count[city_id];
            double mean_sq = global_stats->headway_sq_sum[city_id] /
                             (double)global_stats->headway_count[city_id];
            double variance = mean_sq - mean * mean;
            if (variance < 0.0) {
                variance = 0.0;
            }
            avg_headway = mean;
            headway_cv = mean > 0.0 ? sqrt(variance) / mean : 0.0;
        }

        printf("City %" PRIu32 ": departures=%" PRIu64
               " peak_share=%.4f avg_trip_duration=%.2f avg_headway=%.2f "
               "headway_cv=%.4f avg_stop_count=%.2f\n",
               city_id,
               global_stats->total_departures[city_id],
               peak_share,
               avg_duration,
               avg_headway,
               headway_cv,
               avg_stop_count);
    }

    if (options->emit_csv) {
        printf("CSVROW,%s,%d,%" PRIu64 ",%" PRIu32 ",%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,SUCCESS\n",
               options->backend == BACKEND_CUDA ? "cuda" : "cpu",
               nranks,
               metrics->records_analyzed,
               metrics->city_count,
               metrics->io_seconds,
               metrics->boundary_seconds,
               metrics->compute_seconds,
               metrics->reduce_seconds,
               metrics->total_seconds,
               metrics->mean_peak_share,
               metrics->mean_headway_seconds,
               metrics->mean_headway_cv);
    }
}

int main(int argc, char **argv)
{
    options_t options;
    trip_record_t *local_records = NULL;
    uint64_t requested_records = 0;
    uint64_t total_records = 0;
    uint64_t local_offset = 0;
    uint64_t local_count = 0;
    uint64_t total_start_ticks;
    uint64_t total_end_ticks;
    double local_io_seconds = 0.0;
    double local_boundary_seconds = 0.0;
    double local_compute_seconds = 0.0;
    double local_reduce_seconds = 0.0;
    double global_io_seconds = 0.0;
    double global_boundary_seconds = 0.0;
    double global_compute_seconds = 0.0;
    double global_reduce_seconds = 0.0;
    int rank = 0;
    int nranks = 0;
    int local_max_city = -1;
    int global_max_city = -1;
    uint32_t city_count = 0;
    uint32_t bucket_count = 0;
    int gpu_id = -1;
    analysis_stats_t local_stats;
    analysis_stats_t global_stats;
    boundary_record_t previous_boundary;
    run_metrics_t metrics;
    uint64_t compute_start_ticks;
    uint64_t compute_end_ticks;
    uint64_t reduce_start_ticks;
    uint64_t reduce_end_ticks;
    uint64_t records_analyzed = 0ULL;
    int bucket_slots;
    int route_slots;
    int city_slots;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nranks);

    if (!parse_options(argc, argv, &options)) {
        if (rank == 0) {
            print_usage(argv[0]);
        }
        MPI_Finalize();
        return 1;
    }

    requested_records = options.requested_records;
    bucket_count = 86400U / options.bucket_width_seconds;
    total_start_ticks = clock_now();

    if (!read_trip_records(MPI_COMM_WORLD,
                           options.input_path,
                           requested_records,
                           &local_records,
                           &total_records,
                           &local_offset,
                           &local_count,
                           &local_io_seconds)) {
        MPI_Finalize();
        return 1;
    }

    if (local_count > 0ULL) {
        local_max_city = (int)local_records[local_count - 1ULL].city_id;
    }
    MPI_Allreduce(&local_max_city, &global_max_city, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    city_count = (global_max_city >= 0) ? (uint32_t)(global_max_city + 1) : 0U;

    stats_alloc(&local_stats, city_count, bucket_count, TRANSIT_ROUTE_TYPE_BINS);
    if (rank == 0) {
        stats_alloc(&global_stats, city_count, bucket_count, TRANSIT_ROUTE_TYPE_BINS);
    } else {
        stats_zero(&global_stats);
    }

    previous_boundary = exchange_previous_record(MPI_COMM_WORLD,
                                                 local_records,
                                                 local_count,
                                                 &local_boundary_seconds);

    if (options.backend == BACKEND_CUDA) {
        gpu_id = select_gpu(MPI_COMM_WORLD);
        if (gpu_id < 0) {
            if (rank == 0) {
                fprintf(stderr, "Unable to select a CUDA device on this node\n");
            }
            free(local_records);
            stats_free(&local_stats);
            if (rank == 0) {
                stats_free(&global_stats);
            }
            MPI_Finalize();
            return 1;
        }
        transit_cuda_init(gpu_id, city_count, bucket_count, TRANSIT_ROUTE_TYPE_BINS);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    compute_start_ticks = clock_now();
    if (options.backend == BACKEND_CUDA) {
        transit_analyze_cuda(local_records,
                             (size_t)local_count,
                             &previous_boundary.record,
                             previous_boundary.valid,
                             city_count,
                             bucket_count,
                             TRANSIT_ROUTE_TYPE_BINS,
                             &local_stats);
    } else {
        transit_analyze_cpu(local_records,
                            (size_t)local_count,
                            &previous_boundary.record,
                            previous_boundary.valid,
                            city_count,
                            bucket_count,
                            TRANSIT_ROUTE_TYPE_BINS,
                            &local_stats);
    }
    compute_end_ticks = clock_now();
    local_compute_seconds = clock_ticks_to_seconds(compute_end_ticks - compute_start_ticks);

    city_slots = (int)city_count;
    bucket_slots = (int)(city_count * bucket_count);
    route_slots = (int)(city_count * TRANSIT_ROUTE_TYPE_BINS);

    MPI_Barrier(MPI_COMM_WORLD);
    reduce_start_ticks = clock_now();
    reduce_u64_array(MPI_COMM_WORLD,
                     local_stats.departures_per_bucket,
                     rank == 0 ? global_stats.departures_per_bucket : NULL,
                     bucket_slots);
    reduce_u64_array(MPI_COMM_WORLD,
                     local_stats.route_type_counts,
                     rank == 0 ? global_stats.route_type_counts : NULL,
                     route_slots);
    reduce_u64_array(MPI_COMM_WORLD,
                     local_stats.total_departures,
                     rank == 0 ? global_stats.total_departures : NULL,
                     city_slots);
    reduce_u64_array(MPI_COMM_WORLD,
                     local_stats.peak_departures,
                     rank == 0 ? global_stats.peak_departures : NULL,
                     city_slots);
    reduce_u64_array(MPI_COMM_WORLD,
                     local_stats.offpeak_departures,
                     rank == 0 ? global_stats.offpeak_departures : NULL,
                     city_slots);
    reduce_u64_array(MPI_COMM_WORLD,
                     local_stats.headway_count,
                     rank == 0 ? global_stats.headway_count : NULL,
                     city_slots);
    reduce_u64_array(MPI_COMM_WORLD,
                     local_stats.bunch_count,
                     rank == 0 ? global_stats.bunch_count : NULL,
                     city_slots);
    reduce_u64_array(MPI_COMM_WORLD,
                     local_stats.wide_gap_count,
                     rank == 0 ? global_stats.wide_gap_count : NULL,
                     city_slots);
    reduce_u64_array(MPI_COMM_WORLD,
                     local_stats.trip_count,
                     rank == 0 ? global_stats.trip_count : NULL,
                     city_slots);
    reduce_u64_array(MPI_COMM_WORLD,
                     local_stats.stop_count_sum,
                     rank == 0 ? global_stats.stop_count_sum : NULL,
                     city_slots);
    reduce_double_array(MPI_COMM_WORLD,
                        local_stats.trip_duration_sum,
                        rank == 0 ? global_stats.trip_duration_sum : NULL,
                        city_slots);
    reduce_double_array(MPI_COMM_WORLD,
                        local_stats.trip_duration_sq_sum,
                        rank == 0 ? global_stats.trip_duration_sq_sum : NULL,
                        city_slots);
    reduce_double_array(MPI_COMM_WORLD,
                        local_stats.headway_sum,
                        rank == 0 ? global_stats.headway_sum : NULL,
                        city_slots);
    reduce_double_array(MPI_COMM_WORLD,
                        local_stats.headway_sq_sum,
                        rank == 0 ? global_stats.headway_sq_sum : NULL,
                        city_slots);
    reduce_end_ticks = clock_now();
    local_reduce_seconds = clock_ticks_to_seconds(reduce_end_ticks - reduce_start_ticks);

    MPI_Reduce(&local_io_seconds, &global_io_seconds, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&local_boundary_seconds,
               &global_boundary_seconds,
               1,
               MPI_DOUBLE,
               MPI_MAX,
               0,
               MPI_COMM_WORLD);
    MPI_Reduce(&local_compute_seconds,
               &global_compute_seconds,
               1,
               MPI_DOUBLE,
               MPI_MAX,
               0,
               MPI_COMM_WORLD);
    MPI_Reduce(&local_reduce_seconds,
               &global_reduce_seconds,
               1,
               MPI_DOUBLE,
               MPI_MAX,
               0,
               MPI_COMM_WORLD);

    total_end_ticks = clock_now();
    records_analyzed = total_records;

    if (options.backend == BACKEND_CUDA) {
        transit_cuda_finalize();
    }

    if (rank == 0) {
        metrics = summarize_metrics(&global_stats,
                                    city_count,
                                    records_analyzed,
                                    global_io_seconds,
                                    global_boundary_seconds,
                                    global_compute_seconds,
                                    global_reduce_seconds,
                                    clock_ticks_to_seconds(total_end_ticks - total_start_ticks));
        print_summary(rank, nranks, &options, &metrics, &global_stats);

        if (options.city_report_path != NULL) {
            write_city_report(options.city_report_path, &global_stats, city_count);
        }
        if (options.bucket_report_path != NULL) {
            write_bucket_report(options.bucket_report_path,
                                &global_stats,
                                city_count,
                                bucket_count,
                                options.bucket_width_seconds);
        }
    }

    free(local_records);
    stats_free(&local_stats);
    if (rank == 0) {
        stats_free(&global_stats);
    }

    MPI_Finalize();
    (void)local_offset;
    return 0;
}
