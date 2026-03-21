import { useEffect, useRef, useState } from "react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import {
  Shield,
  FileText,
  Upload,
  Rocket,
  CheckCircle,
  XCircle,
  Loader2,
  type LucideIcon,
} from "lucide-react";

interface LoadingStepperProps {
  isOpen: boolean;
  isComplete: boolean;
  error: string | null;
  onNavigate: () => void;
}

interface StepDef {
  icon: LucideIcon;
  label: string;
  description: string;
}

const STEPS: StepDef[] = [
  { icon: Shield, label: "Validating permissions", description: "Checking access to Genie Space and UC catalog" },
  { icon: FileText, label: "Preparing configuration", description: "Snapshotting current space config and benchmarks" },
  { icon: Upload, label: "Submitting job", description: "Creating Databricks workflow run" },
  { icon: Rocket, label: "Launching pipeline", description: "Starting the 6-task optimization DAG" },
  { icon: CheckCircle, label: "Pipeline started", description: "Redirecting to run detail…" },
];

const STEP_INTERVAL_MS = 800;
const POST_COMPLETE_DELAY_MS = 800;

export function OptimizationLoadingStepper({
  isOpen,
  isComplete,
  error,
  onNavigate,
}: LoadingStepperProps) {
  const [activeStep, setActiveStep] = useState(0);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const navigateTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    if (!isOpen) {
      setActiveStep(0);
      return;
    }
    if (error) return;
    if (isComplete) return;

    if (activeStep < STEPS.length - 2) {
      timerRef.current = setTimeout(() => {
        setActiveStep((s) => s + 1);
      }, STEP_INTERVAL_MS);
    }
    return () => {
      if (timerRef.current) clearTimeout(timerRef.current);
    };
  }, [isOpen, activeStep, error, isComplete]);

  useEffect(() => {
    if (isComplete && isOpen) {
      setActiveStep(STEPS.length - 1);
      navigateTimerRef.current = setTimeout(() => {
        onNavigate();
      }, POST_COMPLETE_DELAY_MS);
    }
    return () => {
      if (navigateTimerRef.current) clearTimeout(navigateTimerRef.current);
    };
  }, [isComplete, isOpen, onNavigate]);

  return (
    <Dialog open={isOpen} onOpenChange={() => {}}>
      <DialogContent
        className="sm:max-w-md [&>button]:hidden"
        onPointerDownOutside={(e) => e.preventDefault()}
        onEscapeKeyDown={(e) => {
          if (!error) e.preventDefault();
        }}
      >
        <DialogHeader>
          <DialogTitle>
            {error ? "Optimization failed" : isComplete ? "Pipeline launched!" : "Starting optimization…"}
          </DialogTitle>
          <DialogDescription>
            {error
              ? "Something went wrong while launching the pipeline."
              : isComplete
                ? "Your optimization run is ready."
                : "Preparing and submitting the optimization pipeline."}
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-1 py-2">
          {STEPS.map((step, i) => {
            const StepIcon = step.icon;
            const isDone = i < activeStep;
            const isActive = i === activeStep && !error;
            const isFailed = i === activeStep && !!error;
            const isFuture = i > activeStep;

            return (
              <div
                key={i}
                className={`flex items-center gap-3 rounded-md px-3 py-2 transition-all duration-300 ${
                  isActive
                    ? "bg-accent/10"
                    : isDone
                      ? ""
                      : isFailed
                        ? "bg-danger/10"
                        : isFuture
                          ? ""
                          : ""
                }`}
              >
                <div className="flex h-8 w-8 shrink-0 items-center justify-center">
                  {isFailed ? (
                    <XCircle className="h-5 w-5 text-danger animate-in fade-in duration-300" />
                  ) : isDone ? (
                    <CheckCircle className="h-5 w-5 text-db-green animate-in fade-in duration-300" />
                  ) : isActive ? (
                    <Loader2 className="h-5 w-5 animate-spin text-db-blue" />
                  ) : (
                    <StepIcon className="h-5 w-5 text-muted" />
                  )}
                </div>
                <div className="min-w-0">
                  <p
                    className={`text-sm font-medium leading-tight ${
                      isFailed
                        ? "text-danger"
                        : isDone
                          ? "text-primary"
                          : isActive
                            ? "text-accent"
                            : "text-muted"
                    }`}
                  >
                    {step.label}
                  </p>
                  {(isActive || isFailed) && (
                    <p className="mt-0.5 text-xs text-muted animate-in fade-in duration-200">
                      {isFailed ? error : step.description}
                    </p>
                  )}
                </div>
              </div>
            );
          })}
        </div>

        {error && (
          <div className="flex justify-end pt-1">
            <Button
              variant="outline"
              size="sm"
              onClick={() => onNavigate()}
            >
              Dismiss
            </Button>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}
