import { Activity } from "lucide-react";

export default function RunsPage() {
  return (
    <div className="p-6 max-w-7xl space-y-6">
      <div className="flex items-center gap-3">
        <Activity size={24} />
        <h1 className="text-2xl font-bold">Pipeline Runs</h1>
      </div>
      <div className="hero bg-base-200 rounded-box py-20">
        <div className="hero-content text-center">
          <div className="max-w-md">
            <h2 className="text-xl font-semibold">Coming Soon</h2>
            <p className="py-4 opacity-60">
              History of pipeline runs with status, duration, and error details.
              Will be powered by <code className="kbd kbd-sm">runs.json</code> from the pipeline.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

