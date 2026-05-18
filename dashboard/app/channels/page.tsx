import { Tv } from "lucide-react";

export default function ChannelsPage() {
  return (
    <div className="p-6 max-w-7xl space-y-6">
      <div className="flex items-center gap-3">
        <Tv size={24} />
        <h1 className="text-2xl font-bold">Channels</h1>
      </div>
      <div className="hero bg-base-200 rounded-box py-20">
        <div className="hero-content text-center">
          <div className="max-w-md">
            <h2 className="text-xl font-semibold">Coming Soon</h2>
            <p className="py-4 opacity-60">
              Top ZZZ content creators ranked by subscribers, views, and video
              output. Will be powered by <code className="kbd kbd-sm">channels.json</code> from the pipeline.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

