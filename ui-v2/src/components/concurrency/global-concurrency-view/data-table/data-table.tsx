import { DataTable } from "@/components/ui/data-table";
import { type GlobalConcurrencyLimit } from "@/hooks/global-concurrency-limits";
import {
	createColumnHelper,
	getCoreRowModel,
	useReactTable,
} from "@tanstack/react-table";

import { ActiveCell } from "./active-cell";

const columnHelper = createColumnHelper<GlobalConcurrencyLimit>();
const columns = [
	columnHelper.accessor("name", {
		header: "Name",
	}),
	columnHelper.accessor("limit", {
		header: "Limit",
	}),
	columnHelper.accessor("active_slots", {
		header: "Active Slots",
	}),
	columnHelper.accessor("slot_decay_per_second", {
		header: "Slots Decay Per Second",
	}),
	columnHelper.accessor("active", {
		header: "Active",
		cell: ActiveCell,
	}),
];

type Props = {
	data: Array<GlobalConcurrencyLimit>;
};

export const GlobalConcurrencyDataTable = ({ data }: Props) => {
	const table = useReactTable({
		data,
		columns,
		getCoreRowModel: getCoreRowModel(),
	});

	return <DataTable table={table} />;
};
