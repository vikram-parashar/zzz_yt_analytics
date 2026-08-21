'use client';
import { useState, useMemo } from 'react';
import Image from 'next/image';
import Link from 'next/link';
import { ATTR_COLORS, fmt } from '@/lib/utils';
import type { AgentStats, SortField, SortDir } from '@/lib/types';
interface AgentGridProps {
  agents: AgentStats[];
  queryTime?: number;
}
export function AgentGrid({ agents, queryTime }: AgentGridProps) {
  const [sortField, setSortField] = useState<SortField>('total_views');
  const [sortDir, setSortDir] = useState<SortDir>('desc');
  const [filterRank, setFilterRank] = useState<string>('all');
  const [filterAttr, setFilterAttr] = useState<string>('all');
  const [filterFaction, setFilterFaction] = useState<string>('all');
  const [search, setSearch] = useState('');
  const factions = useMemo(() => [...new Set(agents.map(a => a.faction).filter(Boolean))].sort(), [agents]);
  const filtered = useMemo(() => {
    let list = [...agents];
    if (filterRank !== 'all') list = list.filter(a => a.rank === filterRank);
    const attributes = ["Electric", "Ice", "Fire", "Physical", "Ether", "Wind","Armorer"]
    if (filterAttr === 'Special') {
      list = list.filter(a => !attributes.includes(a.attribute));
    } else if (filterAttr !== 'all') list = list.filter(a => a.attribute === filterAttr);
    if (filterFaction !== 'all') list = list.filter(a => a.faction === filterFaction);
    if (search) list = list.filter(a => a.name.toLowerCase().includes(search.toLowerCase()));
    const NUMERIC_FIELDS: SortField[] = ['total_views', 'video_count', 'total_likes', 'total_comments'];
    list.sort((a, b) => {
      let va: any = a[sortField];
      let vb: any = b[sortField];
      if (NUMERIC_FIELDS.includes(sortField)) {
        va = Number(va);
        vb = Number(vb);
        if (Number.isNaN(va)) va = 0;
        if (Number.isNaN(vb)) vb = 0;
      } else if (typeof va === 'string' && typeof vb === 'string') {
        va = va.toLowerCase();
        vb = vb.toLowerCase();
      } else {
        va = va ?? 0;
        vb = vb ?? 0;
      }
      if (va < vb) return sortDir === 'asc' ? -1 : 1;
      if (va > vb) return sortDir === 'asc' ? 1 : -1;
      return 0;
    });
    return list;
  }, [agents, filterRank, filterAttr, filterFaction, search, sortField, sortDir]);
  return (
    <section>
      <div className="flex flex-wrap items-center gap-3 mb-4">
        <h2 className="text-2xl font-bold mr-4">Agents</h2>
        {queryTime !== undefined && (
          <span className="text-xs text-base-content/50">Query took {queryTime.toFixed(3)}s</span>
        )}
      </div>
      <div className='my-2 w-1/3'>
        <input type="text" placeholder="Search agent..." className="input input-sm input-bordered w-full" value={search} onChange={e => setSearch(e.target.value)} />
        </div>
      <div className="grid grid-cols-2 md:grid-cols-4 items-center gap-3 mb-4">
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
          <option value="Wind">Wind</option>
          <option value="Armorer">Armorer</option>
          <option value="Special">Special/Unknow</option>
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
        {filtered.map((agent) => {
          const colors = ATTR_COLORS[agent.attribute] ?? ['#666', '#666'];
          return (
            <Link prefetch={false} key={agent.name}
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
                      <span className={`p-2 badge badge-xs text-accent-content ${agent.rank === 'S' ? 'bg-[#d79921]' : agent.rank === 'A' ? 'bg-[#b16286]' : 'badge-ghost'}`}>{agent.rank || '?'}</span> <span className="p-2 badge badge-xs font-bold text-black" style={{
                        background: `linear-gradient( 90deg, ${colors[0]} 0%, ${colors[1]} 100%)`,
                      }}>{agent.attribute || 'Unknown'}</span>
                    </div>
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
          )
        })}
      </div>
    </section>
  );
}