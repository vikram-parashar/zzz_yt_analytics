import { Users } from "lucide-react";

export default function AgentsPage() {
  return (
    <div className="p-6 max-w-7xl space-y-6">
      <div className="flex items-center gap-3">
        <Users size={24} />
        <h1 className="text-2xl font-bold">Agents</h1>
      </div>
      <div className="hero bg-base-200 rounded-box py-20">
        <div className="hero-content text-center">
          <div className="max-w-md">
            <h2 className="text-xl font-semibold">Coming Soon</h2>
            <p className="py-4 opacity-60">
              Agent popularity rankings with historical trends and attribute
              breakdowns. Will be powered by <code className="kbd kbd-sm">agents.json</code> exported
              from the pipeline.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

