import { useState, useEffect } from 'preact/hooks';
import { api } from '../../lib/api';

interface DownloadItem {
  id: string;
  user_email: string;
  dataset_id: string;
  dataset_title: string;
  access_channel: string;
  device_info?: string;
  ip_address: string | null;
  user_agent: string | null;
  downloaded_at: string | null;
}

interface SummaryData {
  total_downloads: number;
  unique_users: number;
  unique_datasets: number;
}

export function DownloadMetricsManager() {
  const [downloads, setDownloads] = useState<DownloadItem[]>([]);
  const [summary, setSummary] = useState<SummaryData>({
    total_downloads: 0,
    unique_users: 0,
    unique_datasets: 0,
  });
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Filters & Pagination
  const [search, setSearch] = useState('');
  const [channel, setChannel] = useState('');
  const [page, setPage] = useState(1);
  const limit = 25;
  const [exporting, setExporting] = useState(false);

  // Well above any realistic audit-log size for this deployment - a single
  // request pulls the whole filtered result set for export rather than
  // paginating through it.
  const EXPORT_LIMIT = 100000;

  const fetchMetrics = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await api.adminGetDownloadMetrics({
        search: search || undefined,
        channel: channel || undefined,
        limit,
        offset: (page - 1) * limit,
      });
      setDownloads(data.downloads || []);
      setSummary(
        data.summary || {
          total_downloads: 0,
          unique_users: 0,
          unique_datasets: 0,
        }
      );
      setTotal(data.total || 0);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Failed to load download metrics');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchMetrics();
  }, [page, channel]);

  const handleSearchSubmit = (e: Event) => {
    e.preventDefault();
    setPage(1);
    fetchMetrics();
  };

  const formatDate = (isoString: string | null) => {
    if (!isoString) return 'N/A';
    try {
      const d = new Date(isoString);
      return d.toLocaleString('en-US', {
        year: 'numeric',
        month: 'short',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: true,
      });
    } catch {
      return isoString;
    }
  };

  const getChannelBadge = (ch: string) => {
    switch (ch.toUpperCase()) {
      case 'SDK':
        return 'bg-purple-50 text-purple-700 border-purple-200';
      case 'MCP':
        return 'bg-emerald-50 text-emerald-700 border-emerald-200';
      case 'WEB':
      default:
        return 'bg-blue-50 text-blue-700 border-blue-200';
    }
  };

  const csvEscape = (value: string | number | null | undefined) => {
    const str = value === null || value === undefined ? '' : String(value);
    return /[",\r\n]/.test(str) ? `"${str.replace(/"/g, '""')}"` : str;
  };

  const handleExportCsv = async () => {
    setExporting(true);
    setError(null);
    try {
      const data = await api.adminGetDownloadMetrics({
        search: search || undefined,
        channel: channel || undefined,
        limit: EXPORT_LIMIT,
        offset: 0,
      });
      const items = data.downloads || [];
      const headers = [
        'Downloaded At',
        'User Email',
        'Dataset ID',
        'Dataset Title',
        'Access Channel',
        'Device Info',
        'IP Address',
        'User Agent',
      ];
      const rows = items.map((item) => [
        item.downloaded_at ?? '',
        item.user_email,
        item.dataset_id,
        item.dataset_title,
        item.access_channel,
        item.device_info ?? '',
        item.ip_address ?? '',
        item.user_agent ?? '',
      ]);
      // Leading BOM so Excel (unlike this table's own CSV *uploads*, this is
      // an export we control end to end) opens non-ASCII dataset titles /
      // emails as UTF-8 instead of guessing the system codepage.
      const csvContent = '﻿' + [headers, ...rows].map((row) => row.map(csvEscape).join(',')).join('\r\n');

      const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `dataset-download-analytics-${new Date().toISOString().slice(0, 10)}.csv`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Failed to export download metrics');
    } finally {
      setExporting(false);
    }
  };

  const totalPages = Math.ceil(total / limit) || 1;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h2 className="text-2xl font-bold text-slate-900 tracking-tight">
            Download Analytics & Audit Log
          </h2>
          <p className="text-sm text-slate-600 mt-1">
            Real-time audit log of all dataset download requests across Web, SDK, and AI Assistant.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={handleExportCsv}
            disabled={exporting || total === 0}
            title={search || channel ? 'Exports the currently filtered results' : 'Exports all download records'}
            className="inline-flex items-center justify-center gap-2 px-4 py-2 text-sm font-medium text-slate-700 bg-white border border-slate-300 rounded-xl hover:bg-slate-50 shadow-sm transition-colors disabled:opacity-50"
          >
            <svg className="w-4 h-4 text-slate-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 3v12m0 0l-4-4m4 4l4-4M4 17v2a2 2 0 002 2h12a2 2 0 002-2v-2" />
            </svg>
            {exporting ? 'Exporting…' : 'Export CSV'}
          </button>
          <button
            onClick={fetchMetrics}
            disabled={loading}
            className="inline-flex items-center justify-center gap-2 px-4 py-2 text-sm font-medium text-slate-700 bg-white border border-slate-300 rounded-xl hover:bg-slate-50 shadow-sm transition-colors disabled:opacity-50"
          >
            <svg className={`w-4 h-4 text-slate-500 ${loading ? 'animate-spin' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
            </svg>
            Refresh
          </button>
        </div>
      </div>

      {/* Metric Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="p-5 bg-white rounded-xl border border-slate-200 shadow-sm">
          <div className="flex items-center justify-between">
            <span className="text-xs font-semibold uppercase tracking-wider text-slate-500">Total Downloads</span>
            <div className="p-2.5 bg-blue-50 text-blue-600 rounded-lg">
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
              </svg>
            </div>
          </div>
          <div className="text-3xl font-extrabold text-slate-900 mt-2">
            {summary.total_downloads.toLocaleString()}
          </div>
          <p className="text-xs text-slate-500 mt-1">Total dataset download requests</p>
        </div>

        <div className="p-5 bg-white rounded-xl border border-slate-200 shadow-sm">
          <div className="flex items-center justify-between">
            <span className="text-xs font-semibold uppercase tracking-wider text-slate-500">Unique Downloaders</span>
            <div className="p-2.5 bg-purple-50 text-purple-600 rounded-lg">
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z" />
              </svg>
            </div>
          </div>
          <div className="text-3xl font-extrabold text-slate-900 mt-2">
            {summary.unique_users.toLocaleString()}
          </div>
          <p className="text-xs text-slate-500 mt-1">Distinct user accounts</p>
        </div>

        <div className="p-5 bg-white rounded-xl border border-slate-200 shadow-sm">
          <div className="flex items-center justify-between">
            <span className="text-xs font-semibold uppercase tracking-wider text-slate-500">Datasets Downloaded</span>
            <div className="p-2.5 bg-emerald-50 text-emerald-600 rounded-lg">
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 17v-2m3 2v-4m3 4v-6m2 10H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
              </svg>
            </div>
          </div>
          <div className="text-3xl font-extrabold text-slate-900 mt-2">
            {summary.unique_datasets.toLocaleString()}
          </div>
          <p className="text-xs text-slate-500 mt-1">Distinct datasets accessed</p>
        </div>
      </div>

      {/* Search & Filter Bar */}
      <div className="bg-white p-4 rounded-xl border border-slate-200 shadow-sm">
        <form onSubmit={handleSearchSubmit} className="flex flex-col sm:flex-row gap-3">
          <div className="relative flex-1">
            <input
              type="text"
              placeholder="Search by user email or dataset ID..."
              value={search}
              onInput={(e) => setSearch((e.target as HTMLInputElement).value)}
              className="w-full pl-10 pr-4 py-2.5 bg-white border border-slate-300 rounded-xl text-sm text-slate-900 placeholder:text-slate-400 focus:ring-2 focus:ring-slate-900 focus:border-slate-900"
            />
            <svg className="w-4 h-4 absolute left-3.5 top-3.5 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
            </svg>
          </div>

          <select
            value={channel}
            onChange={(e) => {
              setChannel((e.target as HTMLSelectElement).value);
              setPage(1);
            }}
            className="px-3 py-2.5 bg-white border border-slate-300 rounded-xl text-sm text-slate-900 focus:ring-2 focus:ring-slate-900 focus:border-slate-900"
          >
            <option value="">All Access Channels</option>
            <option value="WEB">Web Platform (WEB)</option>
            <option value="SDK">Python SDK (SDK)</option>
            <option value="MCP">AI Assistant (MCP)</option>
          </select>

          <button
            type="submit"
            className="px-5 py-2.5 bg-slate-900 hover:bg-slate-800 text-white font-medium text-sm rounded-xl transition-colors shadow-sm"
          >
            Search
          </button>
        </form>
      </div>

      {/* Error Alert */}
      {error && (
        <div className="p-4 bg-red-50 border border-red-200 text-red-700 rounded-xl text-sm font-medium">
          {error}
        </div>
      )}

      {/* Download Activity Table */}
      <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead className="bg-slate-50 text-slate-600 font-semibold border-b border-slate-200 uppercase text-xs tracking-wider">
              <tr>
                <th className="px-5 py-3.5">Date & Time</th>
                <th className="px-5 py-3.5">User Email</th>
                <th className="px-5 py-3.5">Dataset ID & Title</th>
                <th className="px-5 py-3.5">Channel</th>
                <th className="px-5 py-3.5">Device & Browser</th>
                <th className="px-5 py-3.5">Client IP</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 text-slate-900">
              {loading && downloads.length === 0 ? (
                <tr>
                  <td colSpan={6} className="px-5 py-12 text-center text-slate-500">
                    <div className="inline-flex items-center gap-2">
                      <svg className="w-5 h-5 animate-spin text-slate-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
                      </svg>
                      Loading download records...
                    </div>
                  </td>
                </tr>
              ) : downloads.length === 0 ? (
                <tr>
                  <td colSpan={6} className="px-5 py-12 text-center text-slate-500">
                    No dataset download records found.
                  </td>
                </tr>
              ) : (
                downloads.map((item) => (
                  <tr key={item.id} className="hover:bg-slate-50/80 transition-colors">
                    <td className="px-5 py-3.5 font-mono text-xs text-slate-500 whitespace-nowrap">
                      {formatDate(item.downloaded_at)}
                    </td>
                    <td className="px-5 py-3.5 font-medium text-slate-900">
                      {item.user_email}
                    </td>
                    <td className="px-5 py-3.5">
                      <div className="font-semibold text-slate-900">{item.dataset_id}</div>
                      <div className="text-xs text-slate-500 truncate max-w-xs">{item.dataset_title}</div>
                    </td>
                    <td className="px-5 py-3.5 whitespace-nowrap">
                      <span className={`px-2.5 py-1 text-xs font-semibold rounded-md border ${getChannelBadge(item.access_channel)}`}>
                        {item.access_channel}
                      </span>
                    </td>
                    <td className="px-5 py-3.5 whitespace-nowrap text-xs font-medium text-slate-700">
                      {item.device_info || 'Unknown Device'}
                    </td>
                    <td className="px-5 py-3.5 font-mono text-xs text-slate-500 whitespace-nowrap">
                      {item.ip_address || '—'}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination Controls */}
        <div className="px-5 py-3.5 border-t border-slate-200 bg-slate-50 flex items-center justify-between">
          <div className="text-xs text-slate-500">
            Showing <span className="font-medium">{downloads.length > 0 ? (page - 1) * limit + 1 : 0}</span> to{' '}
            <span className="font-medium">{Math.min(page * limit, total)}</span> of{' '}
            <span className="font-medium">{total}</span> entries
          </div>

          <div className="flex items-center gap-2">
            <button
              onClick={() => setPage((p) => Math.max(1, p - 1))}
              disabled={page === 1 || loading}
              className="px-3.5 py-1.5 text-xs font-medium text-slate-700 bg-white border border-slate-300 rounded-lg hover:bg-slate-100 disabled:opacity-50 transition-colors"
            >
              Previous
            </button>
            <span className="text-xs font-medium text-slate-600 px-2">
              Page {page} of {totalPages}
            </span>
            <button
              onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
              disabled={page >= totalPages || loading}
              className="px-3.5 py-1.5 text-xs font-medium text-slate-700 bg-white border border-slate-300 rounded-lg hover:bg-slate-100 disabled:opacity-50 transition-colors"
            >
              Next
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
