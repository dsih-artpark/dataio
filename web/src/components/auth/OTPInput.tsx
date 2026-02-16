import { useRef, useState, useCallback, useEffect } from 'preact/hooks';

interface OTPInputProps {
  length: number;
  onComplete: (code: string) => void;
  disabled?: boolean;
}

export default function OTPInput({ length, onComplete, disabled = false }: OTPInputProps) {
  const [values, setValues] = useState<string[]>(Array(length).fill(''));
  const inputRefs = useRef<(HTMLInputElement | null)[]>([]);
  const hasSubmittedRef = useRef(false);

  // Reset submission guard when disabled changes (allows retry after error)
  useEffect(() => {
    if (!disabled) {
      hasSubmittedRef.current = false;
    }
  }, [disabled]);

  // Wrap onComplete to prevent double-submission during single paste/input
  const handleComplete = useCallback((code: string) => {
    if (hasSubmittedRef.current) return;
    hasSubmittedRef.current = true;
    onComplete(code);
  }, [onComplete]);

  const handleChange = (index: number, value: string) => {
    // Only allow digits
    const digit = value.replace(/\D/g, '').slice(-1);

    const newValues = [...values];
    newValues[index] = digit;
    setValues(newValues);

    // Move to next input if digit entered
    if (digit && index < length - 1) {
      inputRefs.current[index + 1]?.focus();
    }

    // Check if complete
    if (newValues.every((v) => v.length === 1)) {
      handleComplete(newValues.join(''));
    }
  };

  const handleKeyDown = (index: number, e: KeyboardEvent) => {
    // Move to previous input on backspace if current is empty
    if (e.key === 'Backspace' && !values[index] && index > 0) {
      inputRefs.current[index - 1]?.focus();
    }
  };

  const handlePaste = (e: ClipboardEvent) => {
    e.preventDefault();
    const pasted = e.clipboardData?.getData('text') || '';
    const digits = pasted.replace(/\D/g, '').slice(0, length);

    if (digits.length > 0) {
      const newValues = Array(length).fill('');
      digits.split('').forEach((digit, i) => {
        if (i < length) newValues[i] = digit;
      });
      setValues(newValues);

      // Focus the next empty input or the last one, and trigger callback if complete
      const nextEmpty = newValues.findIndex((v) => !v);
      if (nextEmpty >= 0) {
        inputRefs.current[nextEmpty]?.focus();
      } else {
        inputRefs.current[length - 1]?.focus();
        handleComplete(newValues.join(''));
      }
    }
  };

  return (
    <div class="flex justify-center gap-3">
      {values.map((value, index) => (
        <input
          key={index}
          ref={(el) => { inputRefs.current[index] = el; }}
          type="text"
          inputMode="numeric"
          maxLength={1}
          value={value}
          onInput={(e) => handleChange(index, (e.target as HTMLInputElement).value)}
          onKeyDown={(e) => handleKeyDown(index, e)}
          onPaste={handlePaste}
          disabled={disabled}
          class="w-12 h-14 text-center text-2xl font-bold border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-primary-500 disabled:bg-gray-50 disabled:text-gray-400"
          autoComplete="one-time-code"
        />
      ))}
    </div>
  );
}
