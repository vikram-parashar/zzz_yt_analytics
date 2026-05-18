import { TrendingUp } from "lucide-react";

export default function TrendsPage() {
  return (
    <div className="p-6 max-w-7xl space-y-6">
      <div className="flex items-center gap-3">
        <TrendingUp size={24} />
        <h1 className="text-2xl font-bold">Trends</h1>
      </div>
      <div className="hero bg-base-200 rounded-box py-20">
        <div className="hero-content text-center">
          <div className="max-w-md">
            <h2 className="text-xl font-semibold">Coming Soon</h2>
            <p className="py-4 opacity-60">
              Meta shift detection — see which agents are rising or declining
              across patches. Will be powered by <code className="kbd kbd-sm">agent_trends.json</code> and{" "}
              <code className="kbd kbd-sm">patches.json</code> from the pipeline.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

