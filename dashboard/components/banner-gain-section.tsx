'use client';
import { useState, useEffect, useMemo, useRef } from 'react';
import {
  BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from 'recharts';
import { ATTR_COLORS, toDateStr, fmt } from '@/lib/utils';
import type { AgentStats } from '@/lib/types';
interface DimPatch {
  version: string; banner_agent: string; banner_start: string; banner_end: string;
}
interface BannerGainRow {
  agent_name: string; view_gain: number | null;
}
interface BannerGainSectionProps {
  agents: AgentStats[];
  patches: DimPatch[];
  factMinDate: string | null;
}
export function BannerGainSection({ agents, patches, factMinDate }: BannerGainSectionProps) {
  const [selectedBannerVersion, setSelectedBannerVersion] = useState<string | null>(null);
  const [bannerGainData, setBannerGainData] = useState<BannerGainRow[]>([]);
  const bannerInitialized = useRef(false);
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
  useEffect(() => {
    if (bannerVersions.length > 0 && !bannerInitialized.current) {
      bannerInitialized.current = true;
      setSelectedBannerVersion(bannerVersions[bannerVersions.length - 1].version);
    }
  }, [bannerVersions]);
  useEffect(() => {
    if (!selectedBannerVersion) return;
    fetch(`/api/banner-gain?version=${encodeURIComponent(selectedBannerVersion)}`)
      .then(r => r.json())
      .then(setBannerGainData)
      .catch(console.error);
  }, [selectedBannerVersion]);
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
  const selectedPatch = bannerVersions.find(v => v.version === selectedBannerVersion);
  return (
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
          {selectedPatch ? (
            <p className="text-sm text-base-content/60 mb-2">
              Banner period: {toDateStr(selectedPatch.banner_start)} → {toDateStr(selectedPatch.banner_end)}
            </p>
          ) : null}
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
                  <a key={d.name} href={`/agent/${encodeURIComponent(d.name)}`}
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
                  </a>
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
  );
}