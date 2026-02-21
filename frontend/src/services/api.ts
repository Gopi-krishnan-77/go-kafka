import type {
    WeekendEvent,
    CreateEventRequest,
    ListResult,
    SearchResult,
    AggregatedStats,
    BrokerStats,
    PipelineInfo,
    Metrics,
} from '../types';

const BASE = '/api';

async function request<T>(path: string, init?: RequestInit): Promise<T> {
    const res = await fetch(`${BASE}${path}`, {
        headers: { 'Content-Type': 'application/json' },
        ...init,
    });
    if (!res.ok) {
        const err = await res.json().catch(() => ({ error: res.statusText }));
        throw new Error(err.error || res.statusText);
    }
    return res.json();
}

export const api = {
    // Events
    createEvent(data: CreateEventRequest) {
        return request<{ status: string; partition: number; offset: number; event: WeekendEvent }>(
            '/events',
            { method: 'POST', body: JSON.stringify(data) }
        );
    },

    listEvents(params?: { page?: number; limit?: number; category?: string; priority?: string }) {
        const q = new URLSearchParams();
        if (params?.page) q.set('page', String(params.page));
        if (params?.limit) q.set('limit', String(params.limit));
        if (params?.category) q.set('category', params.category);
        if (params?.priority) q.set('priority', params.priority);
        const qs = q.toString();
        return request<ListResult>(`/events${qs ? `?${qs}` : ''}`);
    },

    getEvent(id: string) {
        return request<WeekendEvent>(`/events/${id}`);
    },

    deleteEvent(id: string) {
        return request<{ status: string }>(`/events/${id}`, { method: 'DELETE' });
    },

    searchEvents(query: string) {
        return request<SearchResult>(`/events/search?q=${encodeURIComponent(query)}`);
    },

    // Analytics
    getStats() {
        return request<AggregatedStats>('/stats');
    },

    getMetrics() {
        return request<Metrics>('/metrics');
    },

    // Broker
    getBrokerTopics() {
        return request<BrokerStats>('/broker/topics');
    },

    getBrokerDLQ(topic: string) {
        return request<{ topic: string; count: number; messages: unknown[] }>(`/broker/dlq/${topic}`);
    },

    replayMessages(topic: string, fromOffset: number) {
        return request<{ count: number; messages: unknown[] }>('/broker/replay', {
            method: 'POST',
            body: JSON.stringify({ topic, from_offset: fromOffset }),
        });
    },

    // System
    getPipelineInfo() {
        return request<PipelineInfo>('/pipeline');
    },

    healthCheck() {
        return request<{ status: string }>('/healthz');
    },
};
