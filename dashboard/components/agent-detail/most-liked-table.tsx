'use client';
import { useState } from 'react';
import Link from 'next/link';
import { fmt } from '@/lib/utils';
import type { MostLikedVideo } from '@/lib/types';
interface MostLikedTableProps {
  videos: MostLikedVideo[];
  queryTime?: number;
}
export function MostLikedTable({ videos, queryTime }: MostLikedTableProps) {
  const [likedVideoLimit, setLikedVideoLimit] = useState(5);
  return (
    <div className="card bg-base-100 shadow-xl">
      <div className="card-body p-4">
        <h2 className="card-title text-sm">Most Liked Videos (Bayesian)</h2>
        {queryTime !== undefined && (
          <p className="text-xs text-base-content/50">Query took {queryTime.toFixed(3)}s</p>
        )}
        {videos.length > 0 ? (
          <div className="space-y-2">
            <div className="overflow-x-auto">
              <table className="table table-sm">
                <thead>
                  <tr>
                    <th>#</th>
                    <th>Title</th>
                    <th>Views</th>
                    <th>Likes</th>
                  </tr>
                </thead>
                <tbody>
                  {videos.slice(0, likedVideoLimit).map((v, i) => (
                    <tr key={v.video_id}>
                      <td className="text-sm font-bold">{i + 1}</td>
                      <td className="text-sm max-w-[160px] truncate">
                        <Link href={`https://youtube.com/watch?v=${v.video_id}`} target="_blank" rel="noopener noreferrer" className="link link-hover">
                          {v.title}
                        </Link>
                      </td>
                      <td className="text-sm">{fmt(v.view_count ?? 0)}</td>
                      <td className="text-sm">{fmt(v.like_count ?? 0)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {likedVideoLimit < Math.min(videos.length, 50) && (
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
  );
}