'use client';
import { useDuckDB, tableToArray } from '@/lib/duckdb';
import {
  AGENT_STATS_QUERY,
  DIM_PATCH_QUERY,
  topAgentsTimelineQuery,
  bannerAgentGainQuery,
  risingCreatorsQuery,
} from '@/lib/queries';
import type { AgentStats, RisingCreator, SortField, SortDir } from '@/lib/types';
import Image from 'next/image';
import Link from 'next/link';
import { useEffect, useState, useMemo, useCallback, useRef } from 'react';
import {
  LineChart, Line, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend,
} from 'recharts';
const ATTR_COLORS: Record<string, string> = {
  Electric: '#cba6f7', Ice: '#89dceb', Fire: '#fab387',
  Physical: '#f38ba8', Ether: '#f5c2e7', Honed_Edge: '#a6e3a1',
};
const LINE_COLORS = ['#cba6f7', '#fab387', '#a6e3a1', '#f5c2e7', '#89dceb', '#f9e2af', '#f38ba8', '#74c7ec', '#94e2d5', '#b4befe'];
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
interface DimPatch {
  version: string; banner_agent: string; banner_start: string; banner_end: string;
}
interface BannerGainRow {
  agent_name: string; view_gain: number | null;
}
export default function Home() {
  const { loading, error, query, ready } = useDuckDB();
  const [agents, setAgents] = useState<LinkgentStats[]>([]);
  const [topAgentsTimeline, setTopAgentsTimeline] = useState<Linkny[]>([]);
  const [patches, setPatches] = useState<DimPatch[]>([]);
  const [bannerGainData, setBannerGainData] = useState<BannerGainRow[]>([]);
  const [risingCreators, setRisingCreators] = useState<RisingCreator[]>([]);
  const [factMinDate, setFactMinDate] = useState<string | null>(null);
  const now = new Date();
  const sixMonthsAgo = new Date(now.getFullYear(), now.getMonth() - 6, 1);
  const [startMonth, setStartMonth] = useState(toMonthYear(sixMonthsAgo));
  const [endMonth, setEndMonth] = useState(toMonthYear(now));
  const [selectedBannerVersion, setSelectedBannerVersion] = useState<string | null>(null);
  const bannerInitialized = useRef(false);
  const [creatorTimeRange, setCreatorTimeRange] = useState<'week' | 'month' | 'year'>('month');
  const [creatorLimit, setCreatorLimit] = useState(5);
  const [sortField, setSortField] = useState<SortField>('total_views');
  const [sortDir, setSortDir] = useState<SortDir>('desc');
  const [filterRank, setFilterRank] = useState<string>('all');
  const [filterAttr, setFilterAttr] = useState<string>('all');
  const [filterFaction, setFilterFaction] = useState<string>('all');
  const [search, setSearch] = useState('');
  const bannerVersions = useMemo(() => {
    const seen = new Map<string, DimPatch>();
    for (const p of patches) {
      if (!seen.has(p.version)) seen.set(p.version, p);
    }
    let versions = [...seen.values()].sort((a, b) => a.banner_start.localeCompare(b.banner_start));
    if (factMinDate) {
      versions = versions.filter(v => factMinDate < v.banner_start);
    }
    return versions;
  }, [patches, factMinDate]);
  const topAgentsLines = useMemo(() => {
    const byMonth: Record<string, any> = {};
    const agentNames = [...new Set(topAgentsTimeline.map(d => d.agent_name))];
    for (const d of topAgentsTimeline) {
      const m = String(d.month).slice(0, 7);
      if (!byMonth[m]) byMonth[m] = { month: m, label: toMMYY(m) };
      byMonth[m][d.agent_name] = Number(d.video_count ?? 0);
    }
    return { data: Object.values(byMonth).sort((a: any, b: any) => a.month.localeCompare(b.month)), agents: agentNames };
  }, [topAgentsTimeline]);
  const selectedBannerData = useMemo(() => {
    const seen = new Set<string>();
    return bannerGainData
      .sort((a, b) => (b.view_gain ?? 0) - (a.view_gain ?? 0))
      .filter(bg => {
        if (seen.has(bg.agent_name)) return false;
        seen.add(bg.agent_name);
        return true;
      })
      .slice(0, 5)
      .map(bg => ({
        name: bg.agent_name,
        gain: Number(bg.view_gain ?? 0),
        img: agents.find(a => a.name === bg.agent_name)?.img || '',
      }));
  }, [bannerGainData, agents]);
  const factions = useMemo(() => [...new Set(agents.map(a => a.faction).filter(Boolean))].sort(), [agents]);
  const filtered = useMemo(() => {
    let list = [...agents];
    if (filterRank !== 'all') list = list.filter(a => a.rank === filterRank);
    if (filterAttr !== 'all') list = list.filter(a => a.attribute === filterAttr);
    if (filterFaction !== 'all') list = list.filter(a => a.faction === filterFaction);
    if (search) list = list.filter(a => a.name.toLowerCase().includes(search.toLowerCase()));
    list.sort((a, b) => {
      let va: any = a[sortField] ?? 0, vb: any = b[sortField] ?? 0;
      if (typeof va === 'string') { va = va.toLowerCase(); vb = (vb as string).toLowerCase(); }
      if (va < vb) return sortDir === 'asc' ? -1 : 1;
      if (va > vb) return sortDir === 'asc' ? 1 : -1;
      return 0;
    });
    return list;
  }, [agents, filterRank, filterAttr, filterFaction, search, sortField, sortDir]);
  useEffect(() => {
    if (!ready) return;
    (async () => {
      try {
        const [agT, dpT, mnT] = await Promise.all([
          query(AGENT_STATS_QUERY),
          query(DIM_PATCH_QUERY),
          query('SELECT MIN(snapshot_date)::VARCHAR AS mn FROM fact_agent_daily'),
        ]);
        setAgents(tableToArray<LinkgentStats>(agT));
        setPatches(tableToArray<DimPatch>(dpT));
        const mnRows = tableToArray<{ mn: string | null }>(mnT);
        setFactMinDate(mnRows[0]?.mn ?? null);
      } catch (e: any) { console.error('Query error:', e); }
    })();
  }, [ready, query]);
  useEffect(() => {
    if (bannerVersions.length > 0 && !bannerInitialized.current) {
      bannerInitialized.current = true;
      setSelectedBannerVersion(bannerVersions[bannerVersions.length - 1].version);
    }
  }, [bannerVersions]);
  const fetchBannerGain = useCallback(async () => {
    if (!ready || !selectedBannerVersion) return;
    try {
      const t = await query(bannerAgentGainQuery(selectedBannerVersion));
      setBannerGainData(tableToArray<BannerGainRow>(t));
    } catch (e: any) { console.error('Banner gain error:', e); }
  }, [ready, query, selectedBannerVersion]);
  useEffect(() => { fetchBannerGain(); }, [fetchBannerGain]);
  const fetchTimeline = useCallback(async () => {
    if (!ready) return;
    try {
      const sd = fromMonthYear(startMonth).toISOString().slice(0, 10);
      const ed = new Date(fromMonthYear(endMonth).getFullYear(), fromMonthYear(endMonth).getMonth() + 1, 0).toISOString().slice(0, 10);
      const t = await query(topAgentsTimelineQuery(sd, ed));
      setTopAgentsTimeline(tableToArray(t));
    } catch (e: any) { console.error('Timeline error:', e); }
  }, [ready, query, startMonth, endMonth]);
  useEffect(() => { fetchTimeline(); }, [fetchTimeline]);
  const fetchRising = useCallback(async () => {
    if (!ready) return;
    try {
      const t = await query(risingCreatorsQuery(creatorTimeRange));
      setRisingCreators(tableToArray<RisingCreator>(t));
    } catch (e: any) { console.error('Rising creators error:', e); }
  }, [ready, query, creatorTimeRange]);
  useEffect(() => { fetchRising(); }, [fetchRising]);
  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-base-300">
        <span className="loading loading-spinner loading-lg text-primary"></span>
      </div>
    );
  }
  if (error) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-base-300">
        <div className="alert alert-error max-w-md"><span>{error}</span></div>
      </div>
    );
  }
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
        <section>
          <h2 className="text-2xl font-bold mb-4">Agent Popularity & Trends</h2>
          <div className="card bg-base-100 shadow-xl">
            <div className="card-body p-4">
              <div className="flex flex-wrap items-center gap-3">
                <h3 className="card-title text-sm">Top 5 Agents — Videos Published Per Month</h3>
                <div className="flex gap-2 ml-auto items-center">
                  <label className="text-xs text-base-content/60">From:</label>
                  <input type="month" className="input input-sm input-bordered w-36"
                    value={startMonth} onChange={e => setStartMonth(e.target.value)} />
                  <label className="text-xs text-base-content/60">To:</label>
                  <input type="month" className="input input-sm input-bordered w-36"
                    value={endMonth} onChange={e => setEndMonth(e.target.value)} />
                </div>
              </div>
              <div className="h-64 min-h-[256px]">
                {topAgentsLines.data.length > 0 ? (
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={topAgentsLines.data}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#45475a" />
                      <XAxis dataKey="label" tick={{ fontSize: 10 }} stroke="#7f849c" interval={0} angle={-30} textAnchor="end" height={40} />
                      <YAxis tick={{ fontSize: 10 }} stroke="#7f849c" />
                      <Tooltip />
                      <Legend />
                      {topAgentsLines.agents.map((name, i) => (
                        <Line key={name} type="monotone" dataKey={name}
                          stroke={LINE_COLORS[i % LINE_COLORS.length]}
                          dot={false} strokeWidth={2} />
                      ))}
                    </LineChart>
                  </ResponsiveContainer>
                ) : (
                  <div className="h-full flex items-center justify-center text-base-content/40 text-sm">
                    No agent daily data yet — collection just started
                  </div>
                )}
              </div>
            </div>
          </div>
        </section>
        <section>
          <h2 className="text-2xl font-bold mb-2">Agents with Sudden Popularity in Banners</h2>
          {bannerVersions.length > 0 && (
            <div className="flex flex-wrap gap-2 mb-4">
              {bannerVersions.map(bv => (
                <button key={bv.version}
                  className={`btn btn-sm ${selectedBannerVersion === bv.version ? 'btn-primary' : 'btn-ghost'}`}
                  onClick={() => setSelectedBannerVersion(bv.version)}>
                  v{bv.version} {bv.banner_agent}
                </button>
              ))}
            </div>
          )}
          <div className="card bg-base-100 shadow-xl">
            <div className="card-body p-4">
              {(() => {
                const bv = bannerVersions.find(v => v.version === selectedBannerVersion);
                return bv ? (
                  <p className="text-sm text-base-content/60 mb-2">
                    Banner period: {toDateStr(bv.banner_start)} → {toDateStr(bv.banner_end)}
                  </p>
                ) : null;
              })()}
              {selectedBannerData.length > 0 ? (
                <>
                  <div className="h-64 min-h-[256px]">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={selectedBannerData} barCategoryGap="20%">
                        <CartesianGrid strokeDasharray="3 3" stroke="#45475a" />
                        <XAxis dataKey="name" tick={{ fontSize: 11 }} stroke="#7f849c" />
                        <YAxis tick={{ fontSize: 10 }} stroke="#7f849c" tickFormatter={v => fmt(v)} />
                        <Tooltip formatter={(v: any) => [fmt(Number(v)) + ' views gained', 'View Gain']} />
                        <Bar dataKey="gain" fill="#cba6f7" radius={[4, 4, 0, 0]} name="View Gain" />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                  <div className="flex flex-wrap gap-4 mt-4 justify-center">
                    {selectedBannerData.map((d, i) => (
                      <Link key={d.name} href={`/agent/${encodeURIComponent(d.name)}`}
                        className="card bg-base-200 shadow-md w-28 hover:shadow-lg transition-shadow no-underline">
                        <div className="card-body p-2 items-center text-center">
                          <div className="avatar">
                            <div className="w-12 h-12 rounded-full">
                              {d.img ? <img src={d.img} alt={d.name} /> : (
                                <div className="bg-neutral text-neutral-content w-12 h-12 rounded-full flex items-center justify-center">
                                  <span className="text-lg font-bold">{d.name[0]}</span>
                                </div>
                              )}
                            </div>
                          </div>
                          <p className="text-xs font-semibold truncate w-full">{d.name}</p>
                          <p className="text-xs text-primary font-bold">+{fmt(d.gain)}</p>
                          <span className="badge badge-xs badge-ghost">#{i + 1}</span>
                        </div>
                      </Link>
                    ))}
                  </div>
                </>
              ) : (
                <div className="h-32 flex items-center justify-center text-base-content/40 text-sm">
                  No agent daily data for this banner period yet
                </div>
              )}
            </div>
          </div>
        </section>
        <section>
          <h2 className="text-2xl font-bold mb-4">Rising ZZZ Creators</h2>
          <div className="card bg-base-100 shadow-xl">
            <div className="card-body p-4">
              <div className="flex flex-wrap items-center gap-3 mb-2">
                <select className="select select-sm select-bordered"
                  value={creatorTimeRange} onChange={e => setCreatorTimeRange(e.target.value as any)}>
                  <option value="week">Past Week</option>
                  <option value="month">Past Month</option>
                  <option value="year">Past Year</option>
                </select>
              </div>
              {risingCreators.length > 0 ? (
                <>
                  <div className="overflow-x-auto">
                    <table className="table table-sm">
                      <thead>
                        <tr>
                          <th>#</th>
                          <th>Channel</th>
                          <th>Videos</th>
                          <th>Score</th>
                          <th>Sub Growth %</th>
                          <th>View Growth %</th>
                          <th>Video Cnt Growth %</th>
                        </tr>
                      </thead>
                      <tbody>
                        {risingCreators.slice(0, creatorLimit).map((ch, i) => (
                          <tr key={i}>
                            <td className="text-sm font-bold">{i + 1}</td>
                            <td className="text-sm">
                              <div className="flex items-center gap-2">
                                {ch.thumbnail ? (
                                  <div className="avatar">
                                    <div className="w-12 h-12 rounded-full">
                                      <Image height={200} width={200} src={ch.thumbnail} alt={ch.channel_name} className="rounded-full" />
                                    </div>
                                  </div>
                                ) : null}
                                <Link href={`https://youtube.com/channel/${ch.channel_id}`} target="_blank" rel="noopener noreferrer" className="link link-hover link-primary">
                                  {ch.channel_name}
                                </Link>
                              </div>
                            </td>
                            <td className="text-sm">{ch.videos_collected ?? '-'}</td>
                            <td className="text-sm font-bold text-primary">{(ch.score ?? 0).toFixed(1)}</td>
                            <td className="text-sm">{(ch.sub_growth ?? 0).toFixed(1)}%</td>
                            <td className="text-sm">{(ch.view_growth ?? 0).toFixed(1)}%</td>
                            <td className="text-sm">{(ch.video_cnt_growth ?? 0).toFixed(1)}%</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  {creatorLimit < Math.min(risingCreators.length, 50) && (
                    <div className="text-center mt-3">
                      <button className="btn btn-sm btn-ghost btn-outline"
                        onClick={() => setCreatorLimit(prev => Math.min(prev + 5, 50))}>
                        +5 More ({Math.min(creatorLimit + 5, 50, risingCreators.length)} of {Math.min(risingCreators.length, 50)})
                      </button>
                    </div>
                  )}
                </>
              ) : (
                <p className="text-sm text-base-content/40 text-center py-8">No creator data yet</p>
              )}
            </div>
          </div>
        </section>
        <section>
          <div className="flex flex-wrap items-center gap-3 mb-4">
            <h2 className="text-2xl font-bold mr-4">Agents</h2>
            <input type="text" placeholder="Search agent..."
              className="input input-sm input-bordered w-48"
              value={search} onChange={e => setSearch(e.target.value)} />
            <select className="select select-sm select-bordered" value={filterRank} onChange={e => setFilterRank(e.target.value)}>
              <option value="all">All Ranks</option>
              <option value="S">S-Rank</option>
              <option value="A">A-Rank</option>
            </select>
            <select className="select select-sm select-bordered" value={filterAttr} onChange={e => setFilterAttr(e.target.value)}>
              <option value="all">All Attributes</option>
              <option value="Electric">Electric</option>
              <option value="Ice">Ice</option>
              <option value="Fire">Fire</option>
              <option value="Physical">Physical</option>
              <option value="Ether">Ether</option>
              <option value="Honed_Edge">Honed Edge</option>
            </select>
            <select className="select select-sm select-bordered" value={filterFaction} onChange={e => setFilterFaction(e.target.value)}>
              <option value="all">All Factions</option>
              {factions.map(f => <option key={f} value={f}>{f}</option>)}
            </select>
            <select className="select select-sm select-bordered"
              value={`${sortField}:${sortDir}`}
              onChange={e => { const [f, d] = e.target.value.split(':'); setSortField(f as SortField); setSortDir(d as SortDir); }}>
              <option value="total_views:desc">Views ↓</option>
              <option value="total_views:asc">Views ↑</option>
              <option value="video_count:desc">Videos ↓</option>
              <option value="total_likes:desc">Likes ↓</option>
              <option value="name:asc">Name A-Z</option>
              <option value="rank:asc">Rank S→A</option>
            </select>
          </div>
          <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-4">
            {filtered.map((agent) => (
              <Link key={agent.name}
                href={`/agent/${encodeURIComponent(agent.name)}`}
                className={`card bg-base-100 shadow-xl hover:shadow-2xl transition-shadow cursor-pointer no-underline ${agent.on_banner ? 'ring-2 ring-primary' : ''}`}>
                <div className="card-body p-3">
                  <div className="flex items-center gap-3">
                    <div className="avatar">
                      <div className="w-12 h-12 rounded-md">
                        {agent.img ?
                              <Image height={200} width={200} src={agent.img} alt={agent.name} /> : (
                            <div className="bg-neutral text-neutral-content w-12 h-12 rounded-full flex items-center justify-center">
                              <span className="text-lg font-bold">{agent.name[0]}</span>
                            </div>
                          )}
                      </div>
                    </div>
                    <div className="flex-1 min-w-0">
                      <h3 className="font-semibold text-sm truncate">{agent.name}</h3>
                      <div className="flex gap-1 mt-1">
                        <span className={`badge badge-xs ${agent.rank === 'S' ? 'badge-warning' : 'badge-ghost'}`}>{agent.rank}</span>
                        <span className="badge badge-xs" style={{ backgroundColor: ATTR_COLORS[agent.attribute] || '#6c7086', color: '#1e1e2e' }}>{agent.attribute}</span>
                      </div>
                      <p className="text-xs text-base-content/50 mt-0.5">{agent.faction}</p>
                    </div>
                  </div>
                  <div className="grid grid-cols-3 gap-1 mt-2 text-center">
                    <div>
                      <div className="text-xs text-base-content/60">Views</div>
                      <div className="font-bold text-sm">{fmt(agent.total_views ?? 0)}</div>
                    </div>
                    <div>
                      <div className="text-xs text-base-content/60">Videos</div>
                      <div className="font-bold text-sm">{agent.video_count ?? 0}</div>
                    </div>
                    <div>
                      <div className="text-xs text-base-content/60">Likes</div>
                      <div className="font-bold text-sm">{fmt(agent.total_likes ?? 0)}</div>
                    </div>
                  </div>
                  {agent.on_banner && <div className="badge badge-primary badge-sm mt-1 w-full">On Banner</div>}
                </div>
              </Link>
            ))}
          </div>
        </section>
      </main>
      <footer className="footer footer-center p-4 bg-base-100 text-base-content/60 mt-8">
        <p className="text-sm">ZZZ YouTube Analytics — Data powered by DuckDB</p>
      </footer>
    </div>
  );
}