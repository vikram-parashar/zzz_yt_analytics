'use client';
import { useState, useEffect } from 'react';
import Image from 'next/image';
import Link from 'next/link';
import type { RisingCreator } from '@/lib/types';
export function RisingCreatorsTable() {
  const [risingCreators, setRisingCreators] = useState<RisingCreator[]>([]);
  const [creatorTimeRange, setCreatorTimeRange] = useState<'week' | 'month' | 'year'>('week');
  const [creatorLimit, setCreatorLimit] = useState(5);
  const [loading, setLoading] = useState(true);
  const [queryTime, setQueryTime] = useState<number | null>(null);
  useEffect(() => {
    setLoading(true);
    setCreatorLimit(5);
    fetch(`/api/rising-creators?timeRange=${creatorTimeRange}`)
      .then(r => r.json())
      .then((res: any) => {
        setRisingCreators(res.data ?? res);
        setQueryTime(res.queryTime ?? null);
        setLoading(false);
      })
      .catch(err => {
        console.error(err);
        setLoading(false);
      });
  }, [creatorTimeRange]);
  return (
    <section>
      <h2 className="text-2xl font-bold mb-4">
        Rising ZZZ Creators
      </h2>
      {queryTime !== null && (
        <p className="text-xs text-base-content/50 mb-2">
          Query took {queryTime.toFixed(3)}s
        </p>
      )}
      <div className="card bg-base-100 shadow-xl">
        <div className="card-body p-4">
          <div className="flex flex-wrap items-center gap-3 mb-2">
            <select
              className="select select-sm select-bordered"
              value={creatorTimeRange}
              onChange={e =>
                setCreatorTimeRange(
                  e.target.value as 'week' | 'month' | 'year'
                )
              }
            >
              <option value="week">Past Week</option>
              <option value="month">Past Month</option>
              <option value="year">Past Year</option>
            </select>
          </div>
          {loading ? (
            <div className="space-y-2">
              {[1, 2, 3, 4, 5].map(i => (
                <div
                  key={i}
                  className="h-8 bg-base-200 rounded animate-pulse"
                />
              ))}
            </div>
          ) : risingCreators.length > 0 ? (
            <>
              <div className="overflow-x-auto">
                <table className="table table-sm">
                  <thead>
                    <tr>
                      <th>#</th>
                      <th>Channel</th>
                      <th>Videos (ZZZ / ALL)</th>
                      <th>Views / New Sub</th>
                    </tr>
                  </thead>
                  <tbody>
                    {risingCreators
                      .slice(0, creatorLimit)
                      .map((ch, i) => (
                        <tr key={ch.channel_id}>
                          <td className="text-sm font-bold">
                            {i + 1}
                          </td>
                          <td className="text-sm">
                            <div className="flex items-center gap-2">
                              {ch.thumbnail ? (
                                <div className="avatar">
                                  <div className="w-12 h-12 rounded-full">
                                    <Image
                                      height={200}
                                      width={200}
                                      src={ch.thumbnail}
                                      alt={ch.channel_name}
                                      className="rounded-full"
                                    />
                                  </div>
                                </div>
                              ) : null}
                              <Link
                                href={`https://youtube.com/channel/${ch.channel_id}`}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="link link-hover link-primary"
                              >
                                {ch.channel_name}
                              </Link>
                            </div>
                          </td>
                          <td className="text-sm">
                            {ch.videos_collected ?? '-'}/
                            {ch.total_videos ?? '-'}
                          </td>
                          <td className="text-sm">
                            {ch.views_per_new_sub != null &&
                            Number.isFinite(Number(ch.views_per_new_sub))
                              ? Number(ch.views_per_new_sub).toLocaleString(
                                  undefined,
                                  {
                                    maximumFractionDigits: 1,
                                  }
                                )
                              : '-'}
                          </td>
                        </tr>
                      ))}
                  </tbody>
                </table>
              </div>
              {creatorLimit <
                Math.min(risingCreators.length, 50) && (
                <div className="text-center mt-3">
                  <button
                    className="btn btn-sm btn-ghost btn-outline"
                    onClick={() =>
                      setCreatorLimit(prev =>
                        Math.min(prev + 5, 50)
                      )
                    }
                  >
                    +5 More (
                    {Math.min(
                      creatorLimit + 5,
                      50,
                      risingCreators.length
                    )}{' '}
                    of {Math.min(risingCreators.length, 50)})
                  </button>
                </div>
              )}
            </>
          ) : (
            <p className="text-sm text-base-content/40 text-center py-8">
              No creator data yet
            </p>
          )}
        </div>
      </div>
    </section>
  );
}