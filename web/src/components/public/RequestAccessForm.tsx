import { useEffect, useRef, useState } from 'preact/hooks';
import { api, ApiRequestError } from '../../lib/api';
import { currentUser, initAuth } from '../../lib/auth';
import { ORGANISATION_TYPES, datasetPath, type CatalogueDatasetDetail } from '../../lib/catalogue';
import { CONTACT_EMAIL } from '../../lib/site';

const MAX_WORDS = 250;
const EMAIL = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/;

type Field = 'full_name' | 'email' | 'organisation' | 'organisation_type' | 'organisation_type_other' | 'purpose' | 'consent';

const label = 'flex flex-col gap-1.5 text-sm font-semibold text-gray-700';
const control =
  'w-full min-h-[44px] px-2.5 py-2 rounded-lg text-[15px] font-normal text-gray-900 bg-white focus:outline-none focus:ring-2 focus:ring-primary-500';
const errorText = 'text-[13px] font-normal text-red-700';

const words = (text: string) => (text.trim() ? text.trim().split(/\s+/).length : 0);

/** "Request access" form for View only and Restricted datasets (dataset from ?dataset=). */
export default function RequestAccessForm() {
  const [dataset, setDataset] = useState<CatalogueDatasetDetail | null>(null);
  const [datasetError, setDatasetError] = useState<string | null>(null);
  const [values, setValues] = useState({
    full_name: '',
    email: '',
    organisation: '',
    organisation_type: '',
    organisation_type_other: '',
    purpose: '',
    consent: false,
  });
  const [errors, setErrors] = useState<Partial<Record<Field, string>>>({});
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [done, setDone] = useState(false);
  const summaryRef = useRef<HTMLDivElement>(null);
  const doneRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const id = new URLSearchParams(window.location.search).get('dataset');
    if (!id) {
      setDatasetError('Open this form from a dataset’s page, using its “Request access” button.');
      return;
    }
    api
      .getCatalogueDataset(id)
      .then(setDataset)
      .catch(() => setDatasetError(`We couldn’t find the dataset “${id}”.`));
    // Prefill from the signed-in account, if any.
    initAuth()
      .then(() => {
        const user = currentUser.value;
        if (user) setValues((v) => ({ ...v, email: v.email || user.email, full_name: v.full_name || user.display_name || '' }));
      })
      .catch(() => undefined);
  }, []);

  useEffect(() => {
    if (done) doneRef.current?.focus();
  }, [done]);

  const set = (field: Field, value: string | boolean) => {
    setValues((v) => ({ ...v, [field]: value }));
    if (errors[field]) setErrors((e) => ({ ...e, [field]: undefined }));
  };

  const validate = (): Partial<Record<Field, string>> => {
    const next: Partial<Record<Field, string>> = {};
    if (!values.full_name.trim()) next.full_name = 'Enter your full name';
    if (!EMAIL.test(values.email.trim())) next.email = 'Enter a valid email address, like name@organisation.org';
    if (!values.organisation.trim()) next.organisation = 'Enter your organisation';
    if (!values.organisation_type) next.organisation_type = 'Choose an organisation type';
    if (values.organisation_type === 'Other' && !values.organisation_type_other.trim())
      next.organisation_type_other = 'Please specify your organisation type';
    const count = words(values.purpose);
    if (!count) next.purpose = 'Describe how you will use the data';
    else if (count > MAX_WORDS) next.purpose = `Please keep it to ${MAX_WORDS} words or fewer (now ${count})`;
    if (!values.consent) next.consent = 'Please confirm to send your request';
    return next;
  };

  const submit = async (event: Event) => {
    event.preventDefault();
    if (!dataset) return;
    const found = validate();
    setErrors(found);
    setSubmitError(null);
    const first = Object.keys(found)[0] as Field | undefined;
    if (first) {
      document.getElementById(`ra-${first}`)?.focus();
      return;
    }
    setSubmitting(true);
    try {
      await api.submitAccessRequest({
        dataset_id: dataset.ds_id,
        full_name: values.full_name.trim(),
        email: values.email.trim(),
        organisation: values.organisation.trim(),
        organisation_type: values.organisation_type,
        organisation_type_other: values.organisation_type === 'Other' ? values.organisation_type_other.trim() : null,
        purpose: values.purpose.trim(),
        consent: values.consent,
      });
      setDone(true);
    } catch (err) {
      if (err instanceof ApiRequestError && err.statusCode === 429) {
        setSubmitError('Too many requests from this connection. Please try again in an hour.');
      } else {
        setSubmitError(err instanceof Error && err.message !== 'Request failed' ? err.message : 'Your request couldn’t be sent. Please try again.');
      }
      summaryRef.current?.focus();
    } finally {
      setSubmitting(false);
    }
  };

  const border = (field: Field) => (errors[field] ? 'border-2 border-red-700' : 'border border-gray-300');
  const describedBy = (field: Field, extra?: string) =>
    [errors[field] ? `ra-${field}-error` : null, extra].filter(Boolean).join(' ') || undefined;
  const count = words(values.purpose);

  if (done && dataset) {
    return (
      <div ref={doneRef} tabIndex={-1} class="bg-white border border-gray-200 rounded-2xl p-7 flex flex-col gap-3 focus:outline-none" role="status">
        <h2 class="m-0 text-2xl font-bold text-gray-900">Request sent</h2>
        <p class="m-0 text-base leading-relaxed text-gray-700">
          Thank you. We’ve emailed a copy to <strong>{values.email.trim()}</strong>. We usually reply within 3 working days.
        </p>
        <a href={datasetPath(dataset.ds_id)} class="self-start font-semibold text-primary-700 hover:text-primary-900">
          ← Back to {dataset.title}
        </a>
      </div>
    );
  }

  if (datasetError) {
    return (
      <div class="bg-white border border-gray-200 rounded-2xl p-7 flex flex-col gap-3">
        <p class="m-0 text-base text-gray-700">{datasetError}</p>
        <a href="/datasets" class="self-start font-semibold text-primary-700 hover:text-primary-900">
          Browse datasets →
        </a>
      </div>
    );
  }

  if (dataset?.access_level === 'DOWNLOAD') {
    return (
      <div class="bg-white border border-gray-200 rounded-2xl p-7 flex flex-col gap-3">
        <p class="m-0 text-base text-gray-700">
          “{dataset.title}” is open: you can download it straight away, no request needed.
        </p>
        <a href={datasetPath(dataset.ds_id)} class="self-start font-semibold text-primary-700 hover:text-primary-900">
          Go to the dataset →
        </a>
      </div>
    );
  }

  return (
    <form noValidate onSubmit={submit} class="bg-white border border-gray-200 rounded-2xl p-5 sm:p-7 flex flex-col gap-[18px]">
      <p class="m-0 text-sm text-gray-600">Fields marked * are required.</p>

      <div ref={summaryRef} tabIndex={-1} class="focus:outline-none" aria-live="assertive">
        {submitError && (
          <p role="alert" class="m-0 p-3 rounded-lg bg-red-50 border border-red-200 text-sm text-red-800">
            {submitError}
          </p>
        )}
      </div>

      <label class={label}>
        Dataset
        <input
          type="text"
          readOnly
          value={dataset ? `${dataset.title} (${dataset.ds_id})` : 'Loading…'}
          class={`${control} border border-gray-300 bg-gray-50 text-gray-600`}
        />
      </label>

      <div class="flex flex-wrap gap-[18px]">
        <label class={`${label} flex-[1_1_240px]`}>
          Full name *
          <input
            id="ra-full_name"
            type="text"
            autoComplete="name"
            required
            maxLength={200}
            value={values.full_name}
            onInput={(e) => set('full_name', (e.target as HTMLInputElement).value)}
            aria-invalid={!!errors.full_name}
            aria-describedby={describedBy('full_name')}
            class={`${control} ${border('full_name')}`}
          />
          {errors.full_name && <span id="ra-full_name-error" class={errorText}>{errors.full_name}</span>}
        </label>
        <label class={`${label} flex-[1_1_240px]`}>
          Work email *
          <input
            id="ra-email"
            type="email"
            autoComplete="email"
            required
            maxLength={254}
            value={values.email}
            onInput={(e) => set('email', (e.target as HTMLInputElement).value)}
            onBlur={() => values.email && !EMAIL.test(values.email.trim()) && setErrors((e) => ({ ...e, email: 'Enter a valid email address, like name@organisation.org' }))}
            aria-invalid={!!errors.email}
            aria-describedby={describedBy('email')}
            class={`${control} ${border('email')}`}
          />
          {errors.email && <span id="ra-email-error" class={errorText}>{errors.email}</span>}
        </label>
      </div>

      <div class="flex flex-wrap gap-[18px]">
        <label class={`${label} flex-[1_1_240px]`}>
          Organisation *
          <input
            id="ra-organisation"
            type="text"
            autoComplete="organization"
            required
            maxLength={300}
            value={values.organisation}
            onInput={(e) => set('organisation', (e.target as HTMLInputElement).value)}
            aria-invalid={!!errors.organisation}
            aria-describedby={describedBy('organisation')}
            class={`${control} ${border('organisation')}`}
          />
          {errors.organisation && <span id="ra-organisation-error" class={errorText}>{errors.organisation}</span>}
        </label>
        <label class={`${label} flex-[1_1_240px]`}>
          Organisation type *
          <select
            id="ra-organisation_type"
            required
            value={values.organisation_type}
            onChange={(e) => set('organisation_type', (e.target as HTMLSelectElement).value)}
            aria-invalid={!!errors.organisation_type}
            aria-describedby={describedBy('organisation_type')}
            class={`${control} ${border('organisation_type')}`}
          >
            <option value="" disabled>
              Choose one
            </option>
            {ORGANISATION_TYPES.map((type) => (
              <option key={type} value={type}>
                {type}
              </option>
            ))}
          </select>
          {errors.organisation_type && <span id="ra-organisation_type-error" class={errorText}>{errors.organisation_type}</span>}
        </label>
      </div>

      {values.organisation_type === 'Other' && (
        <label class={label}>
          Please specify your organisation type *
          <input
            id="ra-organisation_type_other"
            type="text"
            required
            maxLength={200}
            value={values.organisation_type_other}
            onInput={(e) => set('organisation_type_other', (e.target as HTMLInputElement).value)}
            aria-invalid={!!errors.organisation_type_other}
            aria-describedby={describedBy('organisation_type_other')}
            class={`${control} ${border('organisation_type_other')}`}
          />
          {errors.organisation_type_other && (
            <span id="ra-organisation_type_other-error" class={errorText}>
              {errors.organisation_type_other}
            </span>
          )}
        </label>
      )}

      <label class={label}>
        Describe how you will use the data (about 100 words) *
        <textarea
          id="ra-purpose"
          rows={5}
          required
          value={values.purpose}
          onInput={(e) => set('purpose', (e.target as HTMLTextAreaElement).value)}
          aria-invalid={!!errors.purpose}
          aria-describedby={describedBy('purpose', 'ra-purpose-count')}
          class={`${control} ${border('purpose')} font-[inherit]`}
        />
        <span class="flex justify-between gap-3">
          <span>{errors.purpose && <span id="ra-purpose-error" class={errorText}>{errors.purpose}</span>}</span>
          <span id="ra-purpose-count" class={`text-[13px] font-normal ${count > MAX_WORDS ? 'text-red-700' : 'text-gray-600'}`}>
            {count} / {MAX_WORDS} words
          </span>
        </span>
      </label>

      <div class="flex flex-col gap-1">
        <label class="flex items-start gap-2.5 text-sm leading-normal text-gray-700 cursor-pointer">
          <input
            id="ra-consent"
            type="checkbox"
            required
            checked={values.consent}
            onChange={(e) => set('consent', (e.target as HTMLInputElement).checked)}
            aria-invalid={!!errors.consent}
            aria-describedby={describedBy('consent')}
            class="w-[18px] h-[18px] mt-0.5 shrink-0 accent-primary-600"
          />
          <span>I will use the data only for the purpose above and will credit the data owner. *</span>
        </label>
        {errors.consent && <span id="ra-consent-error" class={`${errorText} pl-7`}>{errors.consent}</span>}
      </div>

      <div class="flex flex-wrap items-center gap-x-5 gap-y-3">
        <button
          type="submit"
          disabled={submitting || !dataset}
          class="min-h-[44px] px-[22px] rounded-[10px] bg-primary-600 text-white text-[15px] font-semibold hover:bg-primary-700 disabled:opacity-60 focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500 focus-visible:ring-offset-2"
        >
          {submitting ? 'Sending…' : 'Send request'}
        </button>
        <span class="text-sm text-gray-600">
          We usually reply within 3 working days. If you haven’t heard from us by then, please{' '}
          <a href={`mailto:${CONTACT_EMAIL}`} class="font-semibold text-primary-700 hover:text-primary-900">
            contact us
          </a>
          .
        </span>
      </div>
    </form>
  );
}
