import { Suspense } from 'react';
import { fetchAgentStats, fetchDimPatch, fetchFactMinDate } from '@/lib/fetch';
import { AgentTimelineChart } from '@/components/agent-timeline-chart';
import { BannerGainSection } from '@/components/banner-gain-section';
import { RisingCreatorsTable } from '@/components/rising-creators-table';
import { AgentGrid } from '@/components/agent-grid';
import { ChartSkeleton, TableSkeleton, GridSkeleton } from '@/components/loading-skeleton';
export const revalidate = 3600;
export default async function Home() {
  const [agents, patches, factMinDate] = await Promise.all([
    fetchAgentStats(),
    fetchDimPatch(),
    fetchFactMinDate(),
  ]);
  return (
    <div className="min-h-screen bg-base-300 text-base-content flex flex-col">
      <div className="navbar bg-base-100 shadow-lg sticky top-0 z-50">
        <div className="flex-1">
          <span className="text-xl font-bold text-primary">Zenless Zone Zero on Youtube </span>
        </div>
        <div className="flex-none gap-2">
          <span className="badge badge-ghost">{agents.length} agents</span>
        </div>
      </div>
      <main className="max-w-7xl mx-auto p-4 space-y-6 flex-1">
        <Suspense fallback={<ChartSkeleton />}>
          <AgentTimelineChart agents={agents} />
        </Suspense>
        <Suspense fallback={<ChartSkeleton />}>
          <BannerGainSection agents={agents} patches={patches} factMinDate={factMinDate} />
        </Suspense>
        <Suspense fallback={<TableSkeleton />}>
          <RisingCreatorsTable />
        </Suspense>
        <Suspense fallback={<GridSkeleton />}>
          <AgentGrid agents={agents} />
        </Suspense>
      </main>
      <footer className="footer footer-center p-4 bg-base-100 text-base-content/60 mt-8">
        <p className="text-sm">ZZZ YouTube Analytics — Data powered by DuckDB</p>
      </footer>
    </div>
  );
}