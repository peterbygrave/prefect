import { createFakeWorkPool } from "@/mocks";
import { routerDecorator } from "@/storybook/utils";
import type { Meta, StoryObj } from "@storybook/react";
import type { AutomationTrigger } from "./constants";
import { WorkPoolsStatusDetails } from "./work-pools-status-details";

const ANY_WORK_POOLS_ENTER: AutomationTrigger = {
	type: "event",
	id: "bc1f8369-67fd-4b04-98ba-602bd2f8b075",
	match: { "prefect.resource.id": "prefect.work-pool.*" },
	match_related: {},
	after: [],
	expect: ["prefect.work-pool.not-ready"],
	for_each: ["prefect.resource.id"],
	posture: "Reactive",
	threshold: 1,
	within: 0,
};

const ANY_WORK_POOLS_STAY: AutomationTrigger = {
	type: "event",
	id: "413e09b4-b7cd-42a8-b503-cddcbbd5533f",
	match: { "prefect.resource.id": "prefect.work-pool.*" },
	match_related: {},
	after: ["prefect.work-pool.not-ready"],
	expect: ["prefect.work-pool.ready"],
	for_each: ["prefect.resource.id"],
	posture: "Proactive",
	threshold: 1,
	within: 30,
};

const SPECIFIC_WORK_POOLS_ENTER: AutomationTrigger = {
	type: "event",
	id: "46d8316b-8b3f-441e-b4b5-8cb98ed81ea3",
	match: {
		"prefect.resource.id": [
			"prefect.work-pool.18940945-9107-4d8c-8734-ab2dc839cdba",
			"prefect.work-pool.38ce4d9e-df55-4df6-a65b-38a8f2baf975",
		],
	},
	match_related: {},
	after: [],
	expect: ["prefect.work-pool.not-ready"],
	for_each: ["prefect.resource.id"],
	posture: "Reactive",
	threshold: 1,
	within: 0,
};

const SPECIFIC_WORK_POOLS_STAY: AutomationTrigger = {
	type: "event",
	id: "3f8e54f3-13da-4880-bbca-32c42c2f7688",
	match: {
		"prefect.resource.id": [
			"prefect.work-pool.18940945-9107-4d8c-8734-ab2dc839cdba",
			"prefect.work-pool.38ce4d9e-df55-4df6-a65b-38a8f2baf975",
			"prefect.work-pool.06597581-a4ed-4e1a-a451-d430bd6f33d6",
		],
	},
	match_related: {},
	after: ["prefect.work-pool.paused"],
	expect: ["prefect.work-pool.ready"],
	for_each: ["prefect.resource.id"],
	posture: "Proactive",
	threshold: 1,
	within: 1800,
};

const meta = {
	title: "Components/Automations/WorkPoolsStatusDetails",
	component: StoryComponent,
	decorators: [routerDecorator],
} satisfies Meta<typeof WorkPoolsStatusDetails>;

export default meta;

function StoryComponent() {
	return (
		<div className="flex flex-col gap-4">
			{[
				ANY_WORK_POOLS_ENTER,
				ANY_WORK_POOLS_STAY,
				SPECIFIC_WORK_POOLS_ENTER,
				SPECIFIC_WORK_POOLS_STAY,
			].map((trigger, i) => (
				<WorkPoolsStatusDetails
					key={i}
					trigger={trigger}
					workPools={[createFakeWorkPool(), createFakeWorkPool()]}
				/>
			))}
		</div>
	);
}

export const Story: StoryObj = {
	name: "WorkPoolsStatusDetails",
};
