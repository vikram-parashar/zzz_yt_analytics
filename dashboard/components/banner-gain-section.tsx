'use client';
import { useState, useEffect, useMemo, useRef } from 'react';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts';
import { toDateStr, fmt } from '@/lib/utils';
import type { AgentStats } from '@/lib/types';
interface DimPatch {
  version: string;
  banner_agent: string;
  banner_start: string;
  banner_end: string;
}
interface BannerGainRow {
  agent_name: string;
  view_gain: number | null;
}
interface BannerPatch {
  version: string;
  banner_start: string;
  banner_end: string;
  label: string;
}
interface BannerGainSectionProps {
  agents: AgentStats[];
  patches: DimPatch[];
  factMinDate: string | null;
}
export function BannerGainSection({
  agents,
  patches,
  factMinDate,
}: BannerGainSectionProps) {
  const [selectedPatch, setSelectedPatch] =
    useState<BannerPatch | null>(null);
  const [bannerGainData, setBannerGainData] = useState<
    BannerGainRow[]
  >([]);
  const [queryTime, setQueryTime] = useState<number | null>(null);
  const bannerInitialized = useRef(false);
  const bannerPatches = useMemo(() => {
    if (!factMinDate) return [];
    const unique = new Map<string, BannerPatch>();
    for (const p of patches) {
      if (p.banner_end < factMinDate) continue;
      const key = `${p.banner_start}_${p.banner_end}`;
      if (unique.has(key)) continue;
      const start = new Date(p.banner_start);
      const end = new Date(p.banner_end);
      unique.set(key, {
        version: p.version,
        banner_start: p.banner_start,
        banner_end: p.banner_end,
        label: `${p.version} ${start.toLocaleDateString('en-US', {
          month: 'short',
          year: 'numeric',
        })} - ${end.toLocaleDateString('en-US', {
          month: 'short',
          year: 'numeric',
        })}`,
      });
    }
    return [...unique.values()].sort((a, b) =>
      a.banner_start.localeCompare(b.banner_start)
    );
  }, [patches, factMinDate]);
  useEffect(() => {
    if (
      bannerPatches.length > 0 &&
      !bannerInitialized.current
    ) {
      bannerInitialized.current = true;
      setSelectedPatch(
        bannerPatches[bannerPatches.length - 1]
      );
    }
  }, [bannerPatches]);
  useEffect(() => {
    if (!selectedPatch) return;
    fetch(
      `/api/banner-gain?start=${encodeURIComponent(
        selectedPatch.banner_start
      )}&end=${encodeURIComponent(
        selectedPatch.banner_end
      )}`
    )
      .then(r => r.json())
      .then((res: any) => {
        setBannerGainData(res.data ?? res);
        setQueryTime(res.queryTime ?? null);
      })
      .catch(console.error);
  }, [selectedPatch]);
  const selectedBannerData = useMemo(() => {
    const seen = new Set<string>();
    return bannerGainData
      .sort(
        (a, b) =>
          (b.view_gain ?? 0) - (a.view_gain ?? 0)
      )
      .filter(bg => {
        if (seen.has(bg.agent_name)) return false;
        seen.add(bg.agent_name);
        return true;
      })
      .slice(0, 5)
      .map(bg => ({
        name: bg.agent_name,
        gain: Number(bg.view_gain ?? 0),
        img:
          agents.find(a => a.name === bg.agent_name)
            ?.img || '',
      }));
  }, [bannerGainData, agents]);
  return (
    <section>
      <h2 className="text-2xl font-bold mb-2">
        Agents with Sudden Popularity in Banners
      </h2>
      {queryTime !== null && (
        <p className="text-xs text-base-content/50 mb-2">
          Query took {queryTime.toFixed(3)}s
        </p>
      )}
      {bannerPatches.length > 0 && (
        <div className="flex flex-wrap gap-2 mb-4">
          {bannerPatches.map(patch => (
            <button
              key={`${patch.banner_start}_${patch.banner_end}`}
              className={`btn btn-sm ${selectedPatch?.banner_start ===
                patch.banner_start &&
                selectedPatch?.banner_end ===
                patch.banner_end
                ? 'btn-primary'
                : 'btn-ghost'
                }`}
              onClick={() => setSelectedPatch(patch)}
            >
              {patch.label}
            </button>
          ))}
        </div>
      )}
      <div className="card bg-base-100 shadow-xl">
        <div className="card-body p-4">
          {selectedPatch && (
            <p className="text-sm text-base-content/60 mb-2">
              Banner period:{' '}
              {toDateStr(selectedPatch.banner_start)}
              {' → '}
              {toDateStr(selectedPatch.banner_end)}
            </p>
          )}
          {selectedBannerData.length > 0 ? (
            <>
              <div className="h-64 min-h-[256px]">
                <ResponsiveContainer
                  width="100%"
                  height="100%"
                >
                  <BarChart
                    data={selectedBannerData}
                    barCategoryGap="20%"
                  >
                    <CartesianGrid
                      strokeDasharray="3 3"
                      stroke="#45475a"
                    />
                    <XAxis
                      dataKey="name"
                      tick={{ fontSize: 11 }}
                      stroke="#7f849c"
                    />
                    <YAxis
                      tick={{ fontSize: 10 }}
                      stroke="#7f849c"
                      tickFormatter={v => fmt(v)}
                    />
                    <Tooltip
                      formatter={(v: any) => [
                        `${fmt(Number(v))} views gained`,
                        'View Gain',
                      ]}
                    />
                    <Bar
                      dataKey="gain"
                      fill="#cba6f7"
                      radius={[4, 4, 0, 0]}
                      name="View Gain"
                    />
                  </BarChart>
                </ResponsiveContainer>
              </div>
              <div className="flex flex-wrap gap-4 mt-4 justify-center">
                {selectedBannerData.map((d, i) => (
                  <a
                    key={d.name}
                    href={`/agent/${encodeURIComponent(
                      d.name
                    )}`}
                    className="card bg-base-200 shadow-md w-28 hover:shadow-lg transition-shadow no-underline"
                  >
                    <div className="card-body p-2 items-center text-center">
                      <div className="avatar">
                        <div className="w-12 h-12 rounded-full">
                          {d.img ? (
                            <img
                              src={d.img}
                              alt={d.name}
                            />
                          ) : (
                            <div className="bg-neutral text-neutral-content w-12 h-12 rounded-full flex items-center justify-center">
                              <span className="text-lg font-bold">
                                {d.name[0]}
                              </span>
                            </div>
                          )}
                        </div>
                      </div>
                      <p className="text-xs font-semibold truncate w-full">
                        {d.name}
                      </p>
                      <p className="text-xs text-primary font-bold">
                        +{fmt(d.gain)}
                      </p>
                      <span className="badge badge-xs badge-ghost">
                        #{i + 1}
                      </span>
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