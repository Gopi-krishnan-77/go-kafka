export interface WeekendEvent {
    id: string;
    name: string;
    category: string;
    mood_boost: number;
    tags?: string[];
    fun_score: number;
    priority: string;
    day_of_week: string;
    season: string;
    created_at: string;
    processed_at: string;
    expires_at: string;
}

export interface CreateEventRequest {
    name: string;
    category: string;
    mood_boost: number;
    tags?: string[];
}

export interface ListResult {
    events: WeekendEvent[];
    total: number;
    page: number;
    limit: number;
}

export interface SearchResult {
    query: string;
    count: number;
    results: WeekendEvent[];
}

export interface AggregatedStats {
    total_events: number;
    by_category: Record<string, number>;
    by_priority: Record<string, number>;
    avg_fun_score: number;
    top_categories: { category: string; count: number }[];
    score_distribution: Record<string, number>;
}

export interface BrokerStats {
    topics: Record<string, {
        published: number;
        partitions: number;
        dlq_size: number;
    }>;
}

export interface PipelineInfo {
    stages: string[];
    timeout: string;
}

export interface Metrics {
    [key: string]: number;
}
