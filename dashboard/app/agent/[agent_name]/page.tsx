'use client';
import { useDuckDB, tableToArray } from '@/lib/duckdb';
import {
  AGENT_STATS_QUERY,
  agentVideoTimelineQuery,
  agentEngagementTrendQuery,
  agentBannersQuery,
  agentMostLikedVideoQuery,
  agentMostViewedOnQuery,
  agentCoOccurringQuery,
} from '@/lib/queries';
import type { MostLikedVideo, CoOccurringAgent } from '@/lib/types';
import Image from 'next/image';
import Link from 'next/link';
import { useParams } from 'next/navigation';
import { useEffect, useState, useMemo } from 'react';
import {
  LineChart, Line,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  ReferenceArea,
} from 'recharts';
const ATTR_COLORS: Record<string, string> = {
  Electric: '#cba6f7', Ice: '#89dceb', Fire: '#fab387',
  Physical: '#f38ba8', Ether: '#f5c2e7', Honed_Edge: '#a6e3a1',
};
function toDateStr(v: any): string {
  if (v instanceof Date) return v.toISOString().slice(0, 10);
  if (typeof v === 'string') return v.slice(0, 10);
  return String(v).slice(0, 10);
}
function fmt(n: number): string {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K';
  return n.toString();
}
function toMonthYear(d: Date): string {
  return d.toISOString().slice(0, 7);
}
function toMMYY(ym: string): string {
  const [y, m] = ym.split('-');
  return `${m}-${y.slice(2)}`;
}
function fromMonthYear(my: string): Date {
  const [y, m] = my.split('-').map(Number);
  return new Date(y, m - 1, 1);
}
export default function AgentDetailPage() {
  const { agent_name } = useParams<{ agent_name: string }>();
  const decodedName = decodeURIComponent(agent_name);
  const { loading, error, query, ready } = useDuckDB();
  const [agent, setAgent] = useState<LinkgentStats | null>(null);
  const [videoTimeline, setVideoTimeline] = useState<Linkny[]>([]);
  const [engagementTrend, setEngagementTrend] = useState<Linkny[]>([]);
  const [banners, setBanners] = useState<LinkgentBannerPeriod[]>([]);
  const [mostViewedOn, setMostViewedOn] = useState<LinkgentMostViewedOn[]>([]);
  const [mostLikedVideos, setMostLikedVideos] = useState<MostLikedVideo[]>([]);
  const [likedVideoLimit, setLikedVideoLimit] = useState(5);
  const [coOccurringAgents, setCoOccurringAgents] = useState<CoOccurringAgent[]>([]);
  const now = new Date();
  const sixMonthsAgo = new Date(now.getFullYear(), now.getMonth() - 6, 1);
  const [startMonth, setStartMonth] = useState(toMonthYear(sixMonthsAgo));
  const [endMonth, setEndMonth] = useState(toMonthYear(now));
  const [allAgents, setAllAgents] = useState<LinkgentStats[]>([]);
  useEffect(() => {
    if (!ready || !decodedName) return;
    (async () => {
      try {
        const sd = fromMonthYear(startMonth).toISOString().slice(0, 10);
        const ed = new Date(fromMonthYear(endMonth).getFullYear(), fromMonthYear(endMonth).getMonth() + 1, 0).toISOString().slice(0, 10);
        const [agT, vtT, etT, bnT, mlT, mvT, caT] = await Promise.all([
          query(AGENT_STATS_QUERY),
          query(agentVideoTimelineQuery(decodedName, sd, ed)),
          query(agentEngagementTrendQuery(decodedName)),
          query(agentBannersQuery(decodedName)),
          query(agentMostLikedVideoQuery(decodedName, 50)),
          query(agentMostViewedOnQuery(decodedName)),
          query(agentCoOccurringQuery(decodedName)),
        ]);
        const agents = tableToArray<LinkgentStats>(agT);
        setAllAgents(agents);
        setAgent(agents.find(a => a.name === decodedName) || null);
        setVideoTimeline(tableToArray(vtT));
        setEngagementTrend(tableToArray(etT));
        setBanners(tableToArray<LinkgentBannerPeriod>(bnT));
        setMostLikedVideos(tableToArray<MostLikedVideo>(mlT));
        setMostViewedOn(tableToArray<LinkgentMostViewedOn>(mvT));
        setCoOccurringAgents(tableToArray<CoOccurringAgent>(caT));
      } catch (e: any) { console.error('Query error:', e); }
    })();
  }, [ready, decodedName, query, startMonth, endMonth]);
  const videoTimelineData = useMemo(() => {
    if (!videoTimeline.length) return { data: [], formattedBanners: [] };
    const data = videoTimeline.map(d => {
      const m = String(d.month).slice(0, 7);
      const [y, mo] = m.split('-');
      return { month: m, label: `${mo}-${y.slice(2)}`, video_count: Number(d.video_cnt ?? d.video_count ?? 0) };
    });
    const formattedBanners = banners.map(b => {
      const bs = toDateStr(b.banner_start);
      const be = toDateStr(b.banner_end);
      return {
        version: b.version,
        banner_start: bs,
        banner_end: be,
        x1: toMMYY(bs.slice(0, 7)),
        x2: toMMYY(be.slice(0, 7)),
      };
    });
    return { data, formattedBanners };
  }, [videoTimeline, banners]);
  const engagementData = useMemo(() => {
    if (!engagementTrend.length) return [];
    return engagementTrend.map(d => ({
      date: toDateStr(d.date).slice(5),
      views: Number(d.views ?? 0),
      likes: Number(d.likes ?? 0),
    }));
  }, [engagementTrend]);
  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-base-300">
        <span className="loading loading-spinner loading-lg text-primary"></span>
      </div>
    );
  }
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
        <div className="card bg-base-100 shadow-xl">
          <div className="card-body p-4">
            <h2 className="card-title text-sm">Recent Engagement (Past Month)</h2>
            {engagementData.length > 0 ? (
              <div className="h-48 min-h-[192px]">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={engagementData}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#45475a" />
                    <XAxis dataKey="date" tick={{ fontSize: 9 }} stroke="#7f849c" />
                    <YAxis tick={{ fontSize: 10 }} stroke="#7f849c" tickFormatter={v => fmt(v)} />
                    <Tooltip formatter={(v: any) => fmt(Number(v))} />
                    <Line type="monotone" dataKey="views" stroke="#cba6f7" dot={false} strokeWidth={2} />
                    <Line type="monotone" dataKey="likes" stroke="#a6e3a1" dot={false} strokeWidth={2} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            ) : (
              <p className="text-sm text-base-content/40 text-center py-8">No daily engagement data yet</p>
            )}
          </div>
        </div>
        <div className="card bg-base-100 shadow-xl">
          <div className="card-body p-4">
            <div className="flex flex-wrap items-center gap-3">
              <h2 className="card-title text-sm">Videos Published Per Month</h2>
              <div className="flex gap-2 ml-auto items-center">
                <input type="month" className="input input-sm input-bordered w-36"
                  value={startMonth} onChange={e => setStartMonth(e.target.value)} />
                <span className="text-xs">→</span>
                <input type="month" className="input input-sm input-bordered w-36"
                  value={endMonth} onChange={e => setEndMonth(e.target.value)} />
              </div>
            </div>
            <div className="h-72 min-h-[288px]">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={videoTimelineData.data}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#45475a" />
                  <XAxis dataKey="label" tick={{ fontSize: 10 }} stroke="#7f849c" interval={0} angle={-30} textAnchor="end" height={40} />
                  <YAxis tick={{ fontSize: 10 }} stroke="#7f849c" />
                  <Tooltip />
                  {videoTimelineData.formattedBanners.map((b, i) => (
                    <ReferenceArea key={i} x1={b.x1} x2={b.x2}
                      fill="#f9e2af" fillOpacity={0.15} stroke="#f9e2af" strokeOpacity={0.4} />
                  ))}
                  <Line type="monotone" dataKey="video_count"
                    stroke={ATTR_COLORS[agent.attribute] || '#7f849c'} dot={false} strokeWidth={2} />
                </LineChart>
              </ResponsiveContainer>
            </div>
            {videoTimelineData.formattedBanners.length > 0 && (
              <div className="flex flex-wrap gap-2 mt-2">
                <span className="text-xs text-base-content/60">Banner periods:</span>
                {videoTimelineData.formattedBanners.map((b, i) => (
                  <span key={i} className="badge badge-warning badge-sm badge-outline">
                    v{b.version}: {b.banner_start} → {b.banner_end}
                  </span>
                ))}
              </div>
            )}
          </div>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="card bg-base-100 shadow-xl">
            <div className="card-body p-4">
              <h2 className="card-title text-sm">Most Liked Videos (Bayesian)</h2>
              {mostLikedVideos.length > 0 ? (
                <div className="space-y-2">
                  <div className="overflow-x-auto">
                    <table className="table table-sm">
                      <thead>
                        <tr>
                          <th>#</th>
                          <th>Title</th>
                          <th>Views</th>
                          <th>Likes</th>
                          <th>Score</th>
                        </tr>
                      </thead>
                      <tbody>
                        {mostLikedVideos.slice(0, likedVideoLimit).map((v, i) => (
                          <tr key={v.video_id}>
                            <td className="text-sm font-bold">{i + 1}</td>
                            <td className="text-sm max-w-[160px] truncate">
                              <Link href={`https://youtube.com/watch?v=${v.video_id}`} target="_blank" rel="noopener noreferrer" className="link link-hover">
                                {v.title}
                              </Link>
                            </td>
                            <td className="text-sm">{fmt(v.view_count ?? 0)}</td>
                            <td className="text-sm">{fmt(v.like_count ?? 0)}</td>
                            <td className="text-sm font-bold text-primary">{(v.like_score ?? 0).toFixed(1)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  {likedVideoLimit < Math.min(mostLikedVideos.length, 50) && (
                    <div className="text-center">
                      <button className="btn btn-sm btn-ghost btn-outline"
                        onClick={() => setLikedVideoLimit(prev => Math.min(prev + 5, 50))}>
                        +5 More
                      </button>
                    </div>
                  )}
                </div>
              ) : (
                <p className="text-sm text-base-content/40 text-center py-8">No liked video data yet</p>
              )}
            </div>
          </div>
          <div className="card bg-base-100 shadow-xl">
            <div className="card-body p-4">
              <h2 className="card-title text-sm">Top Channels by Views</h2>
              {mostViewedOn.length > 0 ? (
                <div className="overflow-x-auto">
                  <table className="table table-sm">
                    <thead>
                      <tr>
                        <th>#</th>
                        <th>Channel</th>
                        <th>Total Views</th>
                      </tr>
                    </thead>
                    <tbody>
                      {mostViewedOn.slice(0, 8).map((ch, i) => (
                        <tr key={i}>
                          <td className="text-sm font-bold">{i + 1}</td>
                          <td className="text-sm">
                            <Link href={`https://youtube.com/channel/${ch.channel_id}`} target="_blank" rel="noopener noreferrer" className="link link-hover link-primary">
                              {ch.channel_name}
                            </Link>
                          </td>
                          <td className="text-sm font-bold text-primary">{fmt(ch.total_views ?? 0)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p className="text-sm text-base-content/40 text-center py-8">No channel data</p>
              )}
            </div>
          </div>
        </div>
        <div className="card bg-base-100 shadow-xl">
          <div className="card-body p-4">
            <h2 className="card-title text-sm">Agents Discussed Most Alongside {decodedName}</h2>
            {coOccurringAgents.length > 0 ? (
              <div className="flex flex-wrap gap-3">
                {coOccurringAgents.map((ca, i) => {
                  const caAgent = allAgents.find(a => a.name === ca.agent_name);
                  return (
                    <Link key={ca.agent_name}
                      href={`/agent/${encodeURIComponent(ca.agent_name)}`}
                      className="card bg-base-200 shadow-md w-24 hover:shadow-lg transition-shadow no-underline">
                      <div className="card-body p-2 items-center text-center">
                        <div className="avatar">
                          <div className="w-10 h-10 rounded-md">
                            {caAgent?.img ?
                              <Image height={200} width={200} src={caAgent.img} alt={caAgent.name} /> : (
                                <div className="bg-neutral text-neutral-content w-10 h-10 rounded-full flex items-center justify-center">
                                  <span className="text-sm font-bold">{ca.agent_name[0]}</span>
                                </div>
                              )}
                          </div>
                        </div>
                        <p className="text-xs font-semibold truncate w-full">{ca.agent_name}</p>
                        <p className="text-xs text-primary font-bold">{ca.co_video_count} videos</p>
                        <span className="badge badge-xs badge-ghost">#{i + 1}</span>
                      </div>
                    </Link>
                  );
                })}
              </div>
            ) : (
              <p className="text-sm text-base-content/40 text-center py-8">No co-occurring agent data</p>
            )}
          </div>
        </div>
      </main>
      <footer className="footer footer-center p-4 bg-base-100 text-base-content/60 mt-8">
        <p className="text-sm">ZZZ YouTube Analytics — Data powered by DuckDB</p>
      </footer>
    </div>
  );
}