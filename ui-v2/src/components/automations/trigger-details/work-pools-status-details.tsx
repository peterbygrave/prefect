import { WorkPool } from "@/api/work-pools";
import { Icon } from "@/components/ui/icons";
import { pluralize } from "@/utils";
import { Link } from "@tanstack/react-router";
import humanizeDuration from "humanize-duration";
import {
	AUTOMATION_TRIGGER_EVENT_POSTURE_LABEL,
	type AutomationTrigger,
} from "./constants";

const WORK_POOLS_STATUS_LABELS = {
	not_ready: "not ready",
	ready: "ready",
	paused: "paused",
} as const;
type WorkPoolsStatus = keyof typeof WORK_POOLS_STATUS_LABELS;

const PREFECT_WORK_POOL_STATUS = {
	"prefect.work-pool.ready": "ready",
	"prefect.work-pool.not-ready": "not_ready",
	"prefect.work-pool.paused": "paused",
} as const;
type PrefectWorkPoolStatus = keyof typeof PREFECT_WORK_POOL_STATUS;

const getIsAnyWorkPool = (trigger: AutomationTrigger) => {
	return trigger.match?.["prefect.resource.id"] === "prefect.work-pool.*";
};

type WorkPoolsListProps = { workPools: Array<WorkPool> };
const WorkPoolsList = ({ workPools }: WorkPoolsListProps) => {
	return (
		<div className="flex gap-2">
			<div>{pluralize(workPools.length, "deployment")}</div>
			{workPools.map((workPool, i) => {
				return (
					<div key={workPool.id} className="flex items-center gap-1">
						<Link
							className="text-xs flex items-center"
							to="/work-pools/work-pool/$workPoolName"
							params={{ workPoolName: workPool.name }}
						>
							<Icon id="Cpu" className="h-4 w-4 mr-1" />
							{workPool.name}
						</Link>
						{i < workPools.length - 1 && "or"}
					</div>
				);
			})}
		</div>
	);
};

type WorkPoolsStatusDetailsProps = {
	workPools: Array<WorkPool>;
	trigger: AutomationTrigger;
};
export const WorkPoolsStatusDetails = ({
	workPools,
	trigger,
}: WorkPoolsStatusDetailsProps) => {
	const status = getWorkPoolTriggerStatus(trigger);
	return (
		<div className="flex items-center gap-1 text-sm">
			When{" "}
			{getIsAnyWorkPool(trigger) ? (
				"any work pool"
			) : (
				<WorkPoolsList workPools={workPools} />
			)}{" "}
			{AUTOMATION_TRIGGER_EVENT_POSTURE_LABEL[trigger.posture]}{" "}
			{WORK_POOLS_STATUS_LABELS[status]}
			{trigger.posture === "Proactive" &&
				` for ${humanizeDuration(trigger.within * 1_000)}`}
		</div>
	);
};

function getWorkPoolTriggerStatus(trigger: AutomationTrigger): WorkPoolsStatus {
	// Reactive triggers respond to the presence of the expected events
	if (trigger.posture === "Reactive") {
		const status = trigger.expect?.[0];
		if (!status) {
			throw new Error("'expect' field expected");
		}
		return PREFECT_WORK_POOL_STATUS[status as PrefectWorkPoolStatus];
	}
	// Proactive triggers respond to the absence of those expected events.
	const status = trigger.after?.[0];
	if (!status) {
		throw new Error("'after' field expected");
	}
	return PREFECT_WORK_POOL_STATUS[status as PrefectWorkPoolStatus];
}
