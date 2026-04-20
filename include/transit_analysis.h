#ifndef TRANSIT_ANALYSIS_H
#define TRANSIT_ANALYSIS_H

#include <stddef.h>
#include <stdint.h>

#define TRANSIT_BUCKET_WIDTH_SECONDS 900U
#define TRANSIT_BUCKETS_PER_DAY (86400U / TRANSIT_BUCKET_WIDTH_SECONDS)
#define TRANSIT_ROUTE_TYPE_BINS 16U
#define TRANSIT_PEAK_AM_START (7U * 3600U)
#define TRANSIT_PEAK_AM_END (10U * 3600U)
#define TRANSIT_PEAK_PM_START (16U * 3600U)
#define TRANSIT_PEAK_PM_END (19U * 3600U)
#define TRANSIT_BUNCH_THRESHOLD_SECONDS 300U
#define TRANSIT_WIDE_GAP_THRESHOLD_SECONDS 1200U

typedef struct {
    uint32_t city_id;
    uint32_t feed_id;
    uint32_t route_type;
    uint32_t route_hash;
    uint32_t service_mask;
    uint32_t direction_id;
    uint32_t start_secs;
    uint32_t end_secs;
    uint32_t duration_secs;
    uint32_t stop_count;
    uint32_t trip_hash;
    uint32_t reserved;
} trip_record_t;

typedef struct {
    uint64_t *departures_per_bucket;
    uint64_t *route_type_counts;
    uint64_t *total_departures;
    uint64_t *peak_departures;
    uint64_t *offpeak_departures;
    uint64_t *headway_count;
    uint64_t *bunch_count;
    uint64_t *wide_gap_count;
    uint64_t *trip_count;
    uint64_t *stop_count_sum;
    double *trip_duration_sum;
    double *trip_duration_sq_sum;
    double *headway_sum;
    double *headway_sq_sum;
} analysis_stats_t;

#ifdef __cplusplus
extern "C" {
#endif

int transit_is_peak_window(uint32_t normalized_start_secs);
int transit_same_headway_group(const trip_record_t *lhs, const trip_record_t *rhs);

void transit_analyze_cpu(const trip_record_t *records,
                         size_t record_count,
                         const trip_record_t *previous_record,
                         int has_previous_record,
                         uint32_t city_count,
                         uint32_t bucket_count,
                         uint32_t route_type_bins,
                         analysis_stats_t *stats);

void transit_cuda_init(int gpu_id,
                       uint32_t city_count,
                       uint32_t bucket_count,
                       uint32_t route_type_bins);

int transit_cuda_get_device_count(void);

void transit_analyze_cuda(const trip_record_t *records,
                          size_t record_count,
                          const trip_record_t *previous_record,
                          int has_previous_record,
                          uint32_t city_count,
                          uint32_t bucket_count,
                          uint32_t route_type_bins,
                          analysis_stats_t *stats);

void transit_cuda_finalize(void);

#ifdef __cplusplus
}
#endif

#endif
