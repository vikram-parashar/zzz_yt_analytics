interface StatCardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  badge?: string;
}

export default function StatCard({ title, value, subtitle, badge }: StatCardProps) {
  return (
    <div className="card bg-base-200 shadow-sm">
      <div className="card-body p-4">
        <div className="flex items-center justify-between">
          <h3 className="text-sm font-medium opacity-60">{title}</h3>
          {badge && (
            <span className="badge badge-sm badge-primary">{badge}</span>
          )}
        </div>
        <p className="text-3xl font-bold mt-1">{value}</p>
        {subtitle && (
          <p className="text-xs opacity-50 mt-1">{subtitle}</p>
        )}
      </div>
    </div>
  );
}

