import { useEffect, useState } from 'preact/hooks';
import { api } from '../../lib/api';
import { datasetPath, type AccessRequestRecord, type AccessRequestStatus } from '../../lib/catalogue';

const TABS: { status: AccessRequestStatus | undefined; label: string }[] = [
  { status: 'PENDING', label: 'Pending' },
  { status: 'APPROVED', label: 'Approved' },
  { status: 'REJECTED', label: 'Rejected' },
  { status: undefined, label: 'All' },
];

const STATUS_STYLE: Record<AccessRequestStatus, string> = {
  PENDING: 'bg-amber-100 text-amber-800',
  APPROVED: 'bg-green-100 text-green-800',
  REJECTED: 'bg-gray-100 text-gray-700',
};

const formatDate = (iso: string | null) =>
  iso ? new Date(iso).toLocaleString('en-IN', { dateStyle: 'medium', timeStyle: 'short' }) : '';

/** Admin list of "Request access" submissions with approve / reject. */
export default function AccessRequestManager() {
  const [status, setStatus] = useState<AccessRequestStatus | undefined>('PENDING');
  const [requests, setRequests] = useState<AccessRequestRecord[] | null>(null);
  const [pending, setPending] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [notes, setNotes] = useState<Record<string, string>>({});
  const [message, setMessage] = useState<string | null>(null);

  const load = () => {
    setRequests(null);
    setError(null);
    api
      .getAccessRequests(status)
      .then((response) => {
        setRequests(response.requests);
        setPending(response.pending);
      })
      .catch((err) => setError(err instanceof Error ? err.message : 'Failed to load requests'));
  };

  useEffect(load, [status]);

  const review = async (request: AccessRequestRecord, decision: 'approve' | 'reject') => {
    const verb = decision === 'approve' ? 'Approve' : 'Reject';
    if (!window.confirm(`${verb} ${request.full_name}'s request for ${request.dataset_title ?? request.dataset_id}?`)) return;
    setBusy(request.id);
    setMessage(null);
    try {
      const result = await api.reviewAccessRequest(request.id, decision, notes[request.id]);
      setMessage(
        decision === 'approve'
          ? `Approved. ${request.email} can now download ${request.dataset_id}${result.invited ? ' (invited to create an account)' : ''}.`
          : `Rejected. ${request.email} has been told.`,
      );
      load();
    } catch (err) {
      setMessage(err instanceof Error ? err.message : 'Failed');
    } finally {
      setBusy(null);
    }
  };

  return (
    <div class="space-y-4">
      <div class="flex flex-wrap gap-2" role="tablist" aria-label="Request status">
        {TABS.map((tab) => (
          <button
            key={tab.label}
            type="button"
            role="tab"
            aria-selected={status === tab.status}
            onClick={() => setStatus(tab.status)}
            class={`px-3 py-1.5 rounded-full text-sm font-medium border ${
              status === tab.status ? 'bg-primary-600 text-white border-primary-600' : 'bg-white text-gray-700 border-gray-300 hover:border-gray-400'
            }`}
          >
            {tab.label}
            {tab.status === 'PENDING' && pending > 0 && ` (${pending})`}
          </button>
        ))}
      </div>

      {message && <div class="p-3 rounded-lg bg-primary-50 border border-primary-200 text-sm text-primary-900">{message}</div>}
      {error && <div class="p-3 rounded-lg bg-red-50 border border-red-200 text-sm text-red-800">{error}</div>}
      {!requests && !error && <p class="text-sm text-gray-500">Loading…</p>}
      {requests && requests.length === 0 && <p class="text-sm text-gray-500">No requests here.</p>}

      {requests?.map((request) => (
        <article key={request.id} class="bg-white border border-gray-200 rounded-lg p-4 space-y-3">
          <div class="flex flex-wrap items-start justify-between gap-2">
            <div>
              <h2 class="text-base font-semibold text-gray-900">
                {request.full_name} <span class="font-normal text-gray-600">· {request.email}</span>
              </h2>
              <p class="text-sm text-gray-600">
                {request.organisation} · {request.organisation_type}
                {request.organisation_type_other && `: ${request.organisation_type_other}`}
              </p>
            </div>
            <span class={`px-2 py-0.5 rounded-full text-xs font-semibold ${STATUS_STYLE[request.status]}`}>{request.status}</span>
          </div>
          <p class="text-sm text-gray-700">
            Dataset:{' '}
            <a href={datasetPath(request.dataset_id)} target="_blank" rel="noopener" class="font-medium text-primary-700 hover:underline">
              {request.dataset_title ?? request.dataset_id}
            </a>{' '}
            <span class="text-gray-500">({request.dataset_id})</span>
          </p>
          <p class="text-sm text-gray-800 whitespace-pre-wrap bg-gray-50 rounded p-3">{request.purpose}</p>
          <p class="text-xs text-gray-500">
            Received {formatDate(request.created_at)}
            {request.reviewed_at && ` · ${request.status.toLowerCase()} ${formatDate(request.reviewed_at)} by ${request.reviewed_by}`}
          </p>
          {request.review_note && <p class="text-sm text-gray-600">Note: {request.review_note}</p>}

          {request.status === 'PENDING' && (
            <div class="flex flex-wrap items-end gap-2">
              <label class="flex-1 min-w-[220px] text-xs text-gray-600">
                Note to the requester (optional, sent with a rejection)
                <input
                  type="text"
                  value={notes[request.id] ?? ''}
                  onInput={(e) => setNotes({ ...notes, [request.id]: (e.target as HTMLInputElement).value })}
                  class="mt-1 w-full px-2 py-1.5 border border-gray-300 rounded text-sm"
                />
              </label>
              <button
                type="button"
                disabled={busy === request.id}
                onClick={() => review(request, 'approve')}
                class="px-3 py-1.5 rounded bg-green-600 text-white text-sm font-medium hover:bg-green-700 disabled:opacity-50"
              >
                Approve
              </button>
              <button
                type="button"
                disabled={busy === request.id}
                onClick={() => review(request, 'reject')}
                class="px-3 py-1.5 rounded border border-gray-300 text-sm font-medium text-gray-700 hover:bg-gray-50 disabled:opacity-50"
              >
                Reject
              </button>
            </div>
          )}
        </article>
      ))}
    </div>
  );
}
