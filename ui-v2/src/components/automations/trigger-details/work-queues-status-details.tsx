import { WorkQueue } from "@/api/work-queues";
import { Icon } from "@/components/ui/icons";
import { pluralize } from "@/utils";
import { Link } from "@tanstack/react-router";
import humanizeDuration from "humanize-duration";
import {
	AUTOMATION_TRIGGER_EVENT_POSTURE_LABEL,
	type AutomationTrigger,
} from "./constants";

const WORK_QUEUES_STATUS_LABELS = {
	not_ready: "not ready",
	ready: "ready",
	paused: "paused",
} as const;
type WorkQueueStatus = keyof typeof WORK_QUEUES_STATUS_LABELS;

const PREFECT_WORK_QUEUE_STATUS = {
	"prefect.work-queue.ready": "ready",
	"prefect.work-queue.not-ready": "not_ready",
	"prefect.work-queue.paused": "paused",
} as const;
type PrefectWorkQueueStatus = keyof typeof PREFECT_WORK_QUEUE_STATUS;

const getIsAnyWorkQueue = (trigger: AutomationTrigger) => {
	return (
		trigger.match?.["prefect.resource.id"] === "prefect.work-queue.*" &&
		trigger.match_related &&
		Object.keys(trigger.match_related).length === 0
	);
};

type WorkQueuesListProps = { workQueues: Array<WorkQueue> };
const WorkQueuesList = ({ workQueues }: WorkQueuesListProps) => {
	return (
		<div className="flex gap-2">
			<div>{pluralize(workQueues.length, "work queue")}</div>
			{workQueues.map((workQueue, i) => {
				if (!workQueue.work_pool_name) {
					throw new Error("'work_pool_name' expected");
				}
				return (
					<div key={workQueue.id} className="flex items-center gap-1">
						<Link
							className="text-xs flex items-center"
							to="/work-pools/work-pool/$workPoolName/queue/$workQueueName"
							params={{
								workPoolName: workQueue.work_pool_name,
								workQueueName: workQueue.name,
							}}
						>
							<Icon id="Cpu" className="h-4 w-4 mr-1" />
							{workQueue.name}
						</Link>
						{i < workQueues.length - 1 && "or"}
					</div>
				);
			})}
		</div>
	);
};

type WorkQueuesStatusDetailsProps = {
	workQueues: Array<WorkQueue>;
	trigger: AutomationTrigger;
};
export const WorkQueuesStatusDetails = ({
	workQueues,
	trigger,
}: WorkQueuesStatusDetailsProps) => {
	const status = getWorkQueueTriggerStatus(trigger);

	// Any Work Queue
	// Any Work Queue from pools
	// Specified Work Queues

	// Any work queue
	if (getIsAnyWorkQueue(trigger)) {
		return (
			<div className="flex items-center gap-1 text-sm">
				When any work queue
				{AUTOMATION_TRIGGER_EVENT_POSTURE_LABEL[trigger.posture]}{" "}
				{WORK_QUEUES_STATUS_LABELS[status]}
				{trigger.posture === "Proactive" &&
					` for ${humanizeDuration(trigger.within * 1_000)}`}
			</div>
		);
	}

	// Any work queue from pools

	return (
		<div className="flex items-center gap-1 text-sm">
			When{" "}
			{getIsAnyWorkQueue(trigger) ? (
				"any work queue"
			) : (
				<WorkQueuesList workQueues={workQueues} />
			)}{" "}
			{AUTOMATION_TRIGGER_EVENT_POSTURE_LABEL[trigger.posture]}{" "}
			{WORK_QUEUES_STATUS_LABELS[status]}
			{trigger.posture === "Proactive" &&
				` for ${humanizeDuration(trigger.within * 1_000)}`}
		</div>
	);
};

function getWorkQueueTriggerStatus(
	trigger: AutomationTrigger,
): WorkQueueStatus {
	// Reactive triggers respond to the presence of the expected events
	if (trigger.posture === "Reactive") {
		const status = trigger.expect?.[0];
		if (!status) {
			throw new Error("'expect' field expected");
		}
		return PREFECT_WORK_QUEUE_STATUS[status as PrefectWorkQueueStatus];
	}
	// Proactive triggers respond to the absence of those expected events.
	const status = trigger.after?.[0];
	if (!status) {
		throw new Error("'after' field expected");
	}
	return PREFECT_WORK_QUEUE_STATUS[status as PrefectWorkQueueStatus];
}
