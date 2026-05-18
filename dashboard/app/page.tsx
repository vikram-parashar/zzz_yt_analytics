import { fetchDashboardData, formatNumber, formatDate } from "@/lib/data";
import StatCard from "@/components/stat-card";
import AgentTable from "@/components/agent-table";
import TrendChart from "@/components/trend-chart";

export const dynamic = "force-dynamic";

export default async function HomePage() {
  const data = await fetchDashboardData();

  return (
    <div className="p-6 max-w-7xl space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Dashboard</h1>
          <p className="text-sm opacity-50">
            Last updated: {formatDate(data.last_updated)}
          </p>
        </div>
        <span className="badge badge-outline badge-sm">
          {data.pipeline_status}
        </span>
      </div>

      {/* Stat Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard
          title="Agents"
          value={data.summary.total_agents}
          subtitle="Tracked characters"
        />
        <StatCard
          title="Videos"
          value={formatNumber(data.summary.total_videos)}
          subtitle="Discovered on YouTube"
        />
        <StatCard
          title="Channels"
          value={data.summary.total_channels}
          subtitle="Content creators"
        />
        <StatCard
          title="Fact Rows"
          value={formatNumber(data.summary.total_facts)}
          subtitle="Daily engagement snapshots"
        />
      </div>

      {/* Charts + Table Row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <TrendChart data={data.daily_views_trend} />
        <AgentTable agents={data.top_agents} />
      </div>
    </div>
  );
}

