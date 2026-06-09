export function ChartSkeleton() {
  return (
    <div className="card bg-base-100 shadow-xl">
      <div className="card-body p-4">
        <div className="h-4 w-48 bg-base-200 rounded animate-pulse mb-4" />
        <div className="h-64 bg-base-200 rounded animate-pulse" />
      </div>
    </div>
  );
}
export function TableSkeleton() {
  return (
    <div className="card bg-base-100 shadow-xl">
      <div className="card-body p-4">
        <div className="h-4 w-36 bg-base-200 rounded animate-pulse mb-4" />
        <div className="space-y-2">
          {[1, 2, 3, 4, 5].map(i => (
            <div key={i} className="h-8 bg-base-200 rounded animate-pulse" />
          ))}
        </div>
      </div>
    </div>
  );
}
export function GridSkeleton() {
  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-4">
      {[1, 2, 3, 4, 5, 6, 7, 8, 9, 10].map(i => (
        <div key={i} className="card bg-base-100 shadow-xl">
          <div className="card-body p-3">
            <div className="flex items-center gap-3">
              <div className="w-12 h-12 bg-base-200 rounded-md animate-pulse" />
              <div className="flex-1 space-y-2">
                <div className="h-3 w-20 bg-base-200 rounded animate-pulse" />
                <div className="h-2 w-16 bg-base-200 rounded animate-pulse" />
              </div>
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}
export function AgentDetailSkeleton() {
  return (
    <div className="min-h-screen flex items-center justify-center bg-base-300">
      <span className="loading loading-spinner loading-lg text-primary"></span>
    </div>
  );
}