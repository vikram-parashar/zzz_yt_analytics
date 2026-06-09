import { Suspense } from 'react';
import Link from 'next/link';
import Image from 'next/image';
import { fetchAgentStats, fetchAgentBanners, fetchAgentEngagement, fetchAgentVideoTimeline, fetchAgentMostLiked, fetchAgentMostViewedOn, fetchAgentCoOccurring } from '@/lib/fetch';
import { AgentDetailSkeleton } from '@/components/loading-skeleton';
import { EngagementChart } from '@/components/agent-detail/engagement-chart';
import { VideoTimelineChart } from '@/components/agent-detail/video-timeline-chart';
import { MostLikedTable } from '@/components/agent-detail/most-liked-table';
import { TopChannelsTable } from '@/components/agent-detail/top-channels-table';
import { CoOccurringAgents } from '@/components/agent-detail/co-occurring-agents';
import { ATTR_COLORS, fmt } from '@/lib/utils';
export const revalidate = 3600;
interface PageProps {
  params: Promise<{ agent_name: string }>;
}
export default async function AgentDetailPage({ params }: PageProps) {
  const { agent_name } = await params;
  const decodedName = decodeURIComponent(agent_name);
  const now = new Date();
  const sixMonthsAgo = new Date(now.getFullYear(), now.getMonth() - 6, 1);
  const startDate = sixMonthsAgo.toISOString().slice(0, 10);
  const endDate = new Date(now.getFullYear(), now.getMonth() + 1, 0).toISOString().slice(0, 10);
  const [allAgents, videoTimeline, engagement, banners, mostLiked, mostViewedOn, coOccurring] = await Promise.all([
    fetchAgentStats(),
    fetchAgentVideoTimeline(decodedName, startDate, endDate),
    fetchAgentEngagement(decodedName),
    fetchAgentBanners(decodedName),
    fetchAgentMostLiked(decodedName, 50),
    fetchAgentMostViewedOn(decodedName),
    fetchAgentCoOccurring(decodedName),
  ]);
  const agent = allAgents.find(a => a.name === decodedName) || null;
  if (!agent) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-base-300">
        <div className="text-center space-y-4">
          <p className="text-xl">Agent not found</p>
          <Link href="/" className="btn btn-primary">← Back to Dashboard</Link>
        </div>
      </div>
    );
  }
  return (
    <div className="min-h-screen bg-base-300 text-base-content flex flex-col">
      <div className="navbar bg-base-100 shadow-lg sticky top-0 z-50">
        <div className="flex-1">
          <Link href="/" className="btn btn-ghost btn-sm text-primary">← Dashboard</Link>
        </div>
        <div className="flex-none">
          <Link href={`https://youtube.com/results?search_query=${encodeURIComponent(decodedName + ' Zenless Zone Zero')}`}
            target="_blank" rel="noopener noreferrer" className="badge badge-ghost link link-hover">
            {decodedName}
          </Link>
        </div>
      </div>
      <main className="max-w-5xl mx-auto p-4 space-y-6 flex-1">
        <div className="card bg-base-100 shadow-xl">
          <div className="card-body">
            <div className="flex items-center gap-6">
              <div className="avatar">
                <div className="w-20 h-20 rounded-full">
                  {agent.img ?
                    <Image height={200} width={200} src={agent.img} alt={agent.name} /> : (
                      <div className="bg-neutral text-neutral-content w-20 h-20 rounded-full flex items-center justify-center">
                        <span className="text-3xl font-bold">{agent.name[0]}</span>
                      </div>
                    )}
                </div>
              </div>
              <div className="flex-1">
                <h1 className="text-3xl font-bold">{agent.name}</h1>
                <div className="flex flex-wrap gap-2 mt-2">
                  <span className={`badge ${agent.rank === 'S' ? 'badge-warning' : 'badge-ghost'}`}>Rank {agent.rank}</span>
                  <span className="badge" style={{ backgroundColor: ATTR_COLORS[agent.attribute] || '#6c7086', color: '#1e1e2e' }}>{agent.attribute}</span>
                  <span className="badge badge-outline">{agent.speciality}</span>
                  <span className="badge badge-ghost">{agent.faction}</span>
                </div>
              </div>
              {agent.on_banner && <div className="badge badge-primary badge-lg">On Banner Now</div>}
            </div>
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mt-4">
              <div className="text-center">
                <div className="text-xs text-base-content/60">Total Views</div>
                <div className="font-bold text-lg">{fmt(agent.total_views ?? 0)}</div>
              </div>
              <div className="text-center">
                <div className="text-xs text-base-content/60">Total Likes</div>
                <div className="font-bold text-lg">{fmt(agent.total_likes ?? 0)}</div>
              </div>
              <div className="text-center">
                <div className="text-xs text-base-content/60">Videos</div>
                <div className="font-bold text-lg">{agent.video_count ?? 0}</div>
              </div>
              <div className="text-center">
                <div className="text-xs text-base-content/60">Comments</div>
                <div className="font-bold text-lg">{fmt(agent.total_comments ?? 0)}</div>
              </div>
            </div>
          </div>
        </div>
        <Suspense fallback={<AgentDetailSkeleton />}>
          <EngagementChart data={engagement} />
        </Suspense>
        <Suspense fallback={<AgentDetailSkeleton />}>
          <VideoTimelineChart
            agentName={decodedName}
            attribute={agent.attribute}
            initialData={videoTimeline}
            banners={banners}
          />
        </Suspense>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <Suspense fallback={<AgentDetailSkeleton />}>
            <MostLikedTable videos={mostLiked} />
          </Suspense>
          <Suspense fallback={<AgentDetailSkeleton />}>
            <TopChannelsTable channels={mostViewedOn} />
          </Suspense>
        </div>
        <Suspense fallback={<AgentDetailSkeleton />}>
          <CoOccurringAgents agents={allAgents} coOccurring={coOccurring} />
        </Suspense>
      </main>
      <footer className="footer footer-center p-4 bg-base-100 text-base-content/60 mt-8">
        <p className="text-sm">ZZZ YouTube Analytics — Data powered by DuckDB</p>
      </footer>
    </div>
  );
}