import { useQuery, useSuspenseQuery, useMutation } from "@tanstack/react-query";
import type { UseQueryOptions, UseSuspenseQueryOptions, UseMutationOptions } from "@tanstack/react-query";
export class ApiError extends Error {
    status: number;
    statusText: string;
    body: unknown;
    constructor(status: number, statusText: string, body: unknown){
        super(`HTTP ${status}: ${statusText}`);
        this.name = "ApiError";
        this.status = status;
        this.statusText = statusText;
        this.body = body;
    }
}
export interface AccessLevelEntry {
    accessLevel?: string | null;
    spaceId: string;
}
export interface ActionResponse {
    message: string;
    runId: string;
    status: string;
}
export type ActivityItem = Record<string, unknown>;
export type AsiResult = Record<string, unknown>;
export type AsiSummary = Record<string, unknown>;
export interface CheckAccessRequest {
    spaceIds: string[];
}
export type ComparisonData = Record<string, unknown>;
export interface ComplexValue {
    display?: string | null;
    primary?: boolean | null;
    ref?: string | null;
    type?: string | null;
    value?: string | null;
}
export type DimensionScore = Record<string, unknown>;
export interface FunctionInfo {
    catalog: string;
    name: string;
    schema_name: string;
}
export type GateResult = Record<string, unknown>;
export interface HTTPValidationError {
    detail?: ValidationError[];
}
export interface HealthStatus {
    catalog: string;
    catalogExists?: boolean;
    createSchemaCommand?: string | null;
    grantCommand?: string | null;
    healthy: boolean;
    jobHealthy?: boolean;
    jobMessage?: string | null;
    message?: string | null;
    schema: string;
    schemaExists: boolean;
    spClientId?: string | null;
    tablesAccessible: boolean;
    tablesReady: boolean;
    volumeReady?: boolean;
}
export type IterationDetail = Record<string, unknown>;
export type IterationDetailResponse = Record<string, unknown>;
export type IterationSummary = Record<string, unknown>;
export interface JoinInfo {
    joinColumns?: string[];
    leftTable: string;
    relationshipType?: string | null;
    rightTable: string;
}
export interface LeverInfo {
    description: string;
    id: number;
    name: string;
}
export type LeverStatus = Record<string, unknown>;
export interface Name {
    family_name?: string | null;
    given_name?: string | null;
}
export interface PendingReviewItem {
    confidenceTier?: string;
    itemType?: string;
    questionId: string;
    questionText?: string;
    reason?: string;
}
export interface PendingReviewsOut {
    flaggedQuestions?: number;
    items?: PendingReviewItem[];
    labelingSessionUrl?: string | null;
    queuedPatches?: number;
    totalPending?: number;
}
export interface PermissionDashboard {
    experimentBasePath: string;
    frameworkCatalog: string;
    frameworkSchema: string;
    jobName: string;
    jobUrl?: string | null;
    spPrincipalDisplayName?: string | null;
    spPrincipalId: string;
    spaces: SpacePermissions[];
    workspaceHost?: string | null;
}
export interface PipelineLink {
    category: string;
    label: string;
    url: string;
}
export type PipelineRun = Record<string, unknown>;
export type PipelineStep = Record<string, unknown>;
export type ProvenanceRecord = Record<string, unknown>;
export type ProvenanceSummary = Record<string, unknown>;
export type QuestionResult = Record<string, unknown>;
export type ReflectionEntry = Record<string, unknown>;
export type RunStatusResponse = Record<string, unknown>;
export type RunSummary = Record<string, unknown>;
export interface SchemaPermission {
    catalog: string;
    readGrantCommand?: string | null;
    readGranted: boolean;
    schema_name: string;
    writeGrantCommand?: string | null;
    writeGranted: boolean;
}
export interface SpaceConfiguration {
    instructions: string;
    sampleQuestions: string[];
    tableDescriptions: TableDescription[];
}
export interface SpaceDetail {
    benchmarkQuestions?: string[];
    description: string;
    functions?: FunctionInfo[];
    hasActiveRun?: boolean;
    id: string;
    instructions: string;
    joins?: JoinInfo[];
    name: string;
    optimizationHistory: RunSummary[];
    sampleQuestions: string[];
    tables: TableInfo[];
}
export type SpaceListResponse = Record<string, unknown>;
export interface SpacePermissions {
    schemas: SchemaPermission[];
    spDisplayName?: string | null;
    spGrantInstructions?: string | null;
    spHasManage: boolean;
    spaceId: string;
    status: string;
    title: string;
}
export type SpaceSummary = Record<string, unknown>;
export type SuggestionOut = Record<string, unknown>;
export interface SuggestionReviewRequest {
    comment?: string | null;
    status: string;
}
export interface TableDescription {
    description: string;
    tableName: string;
}
export interface TableInfo {
    catalog: string;
    columnCount: number;
    description: string;
    name: string;
    rowCount?: number | null;
    schema_name: string;
}
export interface TriggerRequest {
    apply_mode?: string;
    deploy_target?: string | null;
    levers?: number[] | null;
    space_id: string;
}
export interface TriggerResponse {
    jobRunId: string;
    jobUrl?: string | null;
    runId: string;
    status: string;
}
export interface User {
    active?: boolean | null;
    display_name?: string | null;
    emails?: ComplexValue[] | null;
    entitlements?: ComplexValue[] | null;
    external_id?: string | null;
    groups?: ComplexValue[] | null;
    id?: string | null;
    name?: Name | null;
    roles?: ComplexValue[] | null;
    schemas?: UserSchema[] | null;
    user_name?: string | null;
}
export const UserSchema = {
    "urn:ietf:params:scim:schemas:core:2.0:User": "urn:ietf:params:scim:schemas:core:2.0:User",
    "urn:ietf:params:scim:schemas:extension:workspace:2.0:User": "urn:ietf:params:scim:schemas:extension:workspace:2.0:User"
} as const;
export type UserSchema = typeof UserSchema[keyof typeof UserSchema];
export interface ValidationError {
    ctx?: Record<string, unknown>;
    input?: unknown;
    loc: (string | number)[];
    msg: string;
    type: string;
}
export interface VersionOut {
    version: string;
}
export interface GetActivityParams {
    space_id?: string | null;
    limit?: number;
    "X-Forwarded-Host"?: string | null;
    "X-Forwarded-Preferred-Username"?: string | null;
    "X-Forwarded-User"?: string | null;
    "X-Forwarded-Email"?: string | null;
    "X-Request-Id"?: string | null;
    "X-Forwarded-Access-Token"?: string | null;
}
export const getActivity = async (params?: GetActivityParams, options?: RequestInit): Promise<{
    data: ActivityItem[];
}> =>{
    const searchParams = new URLSearchParams();
    if (params?.space_id != null) searchParams.set("space_id", String(params?.space_id));
    if (params?.limit != null) searchParams.set("limit", String(params?.limit));
    const queryString = searchParams.toString();
    const url = queryString ? `/api/genie/activity?${queryString}` : "/api/genie/activity";
    const res = await fetch(url, {
        ...options,
        method: "GET",
        headers: {
            ...(params?.["X-Forwarded-Host"] != null && {
                "X-Forwarded-Host": params["X-Forwarded-Host"]
            }),
            ...(params?.["X-Forwarded-Preferred-Username"] != null && {
                "X-Forwarded-Preferred-Username": params["X-Forwarded-Preferred-Username"]
            }),
            ...(params?.["X-Forwarded-User"] != null && {
                "X-Forwarded-User": params["X-Forwarded-User"]
            }),
            ...(params?.["X-Forwarded-Email"] != null && {
                "X-Forwarded-Email": params["X-Forwarded-Email"]
            }),
            ...(params?.["X-Request-Id"] != null && {
                "X-Request-Id": params["X-Request-Id"]
            }),
            ...(params?.["X-Forwarded-Access-Token"] != null && {
                "X-Forwarded-Access-Token": params["X-Forwarded-Access-Token"]
            }),
            ...options?.headers
        }
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const getActivityKey = (params?: GetActivityParams)=>{
    return [
        "/api/genie/activity",
        params
    ] as const;
};
export function useGetActivity<TData = {
    data: ActivityItem[];
}>(options?: {
    params?: GetActivityParams;
    query?: Omit<UseQueryOptions<{
        data: ActivityItem[];
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: getActivityKey(options?.params),
        queryFn: ()=>getActivity(options?.params),
        ...options?.query
    });
}
export function useGetActivitySuspense<TData = {
    data: ActivityItem[];
}>(options?: {
    params?: GetActivityParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: ActivityItem[];
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: getActivityKey(options?.params),
        queryFn: ()=>getActivity(options?.params),
        ...options?.query
    });
}
export interface CurrentUserParams {
    "X-Forwarded-Host"?: string | null;
    "X-Forwarded-Preferred-Username"?: string | null;
    "X-Forwarded-User"?: string | null;
    "X-Forwarded-Email"?: string | null;
    "X-Request-Id"?: string | null;
    "X-Forwarded-Access-Token"?: string | null;
}
export const currentUser = async (params?: CurrentUserParams, options?: RequestInit): Promise<{
    data: User;
}> =>{
    const res = await fetch("/api/genie/current-user", {
        ...options,
        method: "GET",
        headers: {
            ...(params?.["X-Forwarded-Host"] != null && {
                "X-Forwarded-Host": params["X-Forwarded-Host"]
            }),
            ...(params?.["X-Forwarded-Preferred-Username"] != null && {
                "X-Forwarded-Preferred-Username": params["X-Forwarded-Preferred-Username"]
            }),
            ...(params?.["X-Forwarded-User"] != null && {
                "X-Forwarded-User": params["X-Forwarded-User"]
            }),
            ...(params?.["X-Forwarded-Email"] != null && {
                "X-Forwarded-Email": params["X-Forwarded-Email"]
            }),
            ...(params?.["X-Request-Id"] != null && {
                "X-Request-Id": params["X-Request-Id"]
            }),
            ...(params?.["X-Forwarded-Access-Token"] != null && {
                "X-Forwarded-Access-Token": params["X-Forwarded-Access-Token"]
            }),
            ...options?.headers
        }
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const currentUserKey = (params?: CurrentUserParams)=>{
    return [
        "/api/genie/current-user",
        params
    ] as const;
};
export function useCurrentUser<TData = {
    data: User;
}>(options?: {
    params?: CurrentUserParams;
    query?: Omit<UseQueryOptions<{
        data: User;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: currentUserKey(options?.params),
        queryFn: ()=>currentUser(options?.params),
        ...options?.query
    });
}
export function useCurrentUserSuspense<TData = {
    data: User;
}>(options?: {
    params?: CurrentUserParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: User;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: currentUserKey(options?.params),
        queryFn: ()=>currentUser(options?.params),
        ...options?.query
    });
}
export const getHealth = async (options?: RequestInit): Promise<{
    data: HealthStatus;
}> =>{
    const res = await fetch("/api/genie/health", {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const getHealthKey = ()=>{
    return [
        "/api/genie/health"
    ] as const;
};
export function useGetHealth<TData = {
    data: HealthStatus;
}>(options?: {
    query?: Omit<UseQueryOptions<{
        data: HealthStatus;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: getHealthKey(),
        queryFn: ()=>getHealth(),
        ...options?.query
    });
}
export function useGetHealthSuspense<TData = {
    data: HealthStatus;
}>(options?: {
    query?: Omit<UseSuspenseQueryOptions<{
        data: HealthStatus;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: getHealthKey(),
        queryFn: ()=>getHealth(),
        ...options?.query
    });
}
export const listLevers = async (options?: RequestInit): Promise<{
    data: LeverInfo[];
}> =>{
    const res = await fetch("/api/genie/levers", {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const listLeversKey = ()=>{
    return [
        "/api/genie/levers"
    ] as const;
};
export function useListLevers<TData = {
    data: LeverInfo[];
}>(options?: {
    query?: Omit<UseQueryOptions<{
        data: LeverInfo[];
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: listLeversKey(),
        queryFn: ()=>listLevers(),
        ...options?.query
    });
}
export function useListLeversSuspense<TData = {
    data: LeverInfo[];
}>(options?: {
    query?: Omit<UseSuspenseQueryOptions<{
        data: LeverInfo[];
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: listLeversKey(),
        queryFn: ()=>listLevers(),
        ...options?.query
    });
}
export interface GetPendingReviewsParams {
    space_id: string;
}
export const getPendingReviews = async (params: GetPendingReviewsParams, options?: RequestInit): Promise<{
    data: PendingReviewsOut;
}> =>{
    const res = await fetch(`/api/genie/pending-reviews/${params.space_id}`, {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const getPendingReviewsKey = (params?: GetPendingReviewsParams)=>{
    return [
        "/api/genie/pending-reviews/{space_id}",
        params
    ] as const;
};
export function useGetPendingReviews<TData = {
    data: PendingReviewsOut;
}>(options: {
    params: GetPendingReviewsParams;
    query?: Omit<UseQueryOptions<{
        data: PendingReviewsOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: getPendingReviewsKey(options.params),
        queryFn: ()=>getPendingReviews(options.params),
        ...options?.query
    });
}
export function useGetPendingReviewsSuspense<TData = {
    data: PendingReviewsOut;
}>(options: {
    params: GetPendingReviewsParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: PendingReviewsOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: getPendingReviewsKey(options.params),
        queryFn: ()=>getPendingReviews(options.params),
        ...options?.query
    });
}
export interface GetRunParams {
    run_id: string;
    "X-Forwarded-Host"?: string | null;
    "X-Forwarded-Preferred-Username"?: string | null;
    "X-Forwarded-User"?: string | null;
    "X-Forwarded-Email"?: string | null;
    "X-Request-Id"?: string | null;
    "X-Forwarded-Access-Token"?: string | null;
}
export const getRun = async (params: GetRunParams, options?: RequestInit): Promise<{
    data: PipelineRun;
}> =>{
    const res = await fetch(`/api/genie/runs/${params.run_id}`, {
        ...options,
        method: "GET",
        headers: {
            ...(params?.["X-Forwarded-Host"] != null && {
                "X-Forwarded-Host": params["X-Forwarded-Host"]
            }),
            ...(params?.["X-Forwarded-Preferred-Username"] != null && {
                "X-Forwarded-Preferred-Username": params["X-Forwarded-Preferred-Username"]
            }),
            ...(params?.["X-Forwarded-User"] != null && {
                "X-Forwarded-User": params["X-Forwarded-User"]
            }),
            ...(params?.["X-Forwarded-Email"] != null && {
                "X-Forwarded-Email": params["X-Forwarded-Email"]
            }),
            ...(params?.["X-Request-Id"] != null && {
                "X-Request-Id": params["X-Request-Id"]
            }),
            ...(params?.["X-Forwarded-Access-Token"] != null && {
                "X-Forwarded-Access-Token": params["X-Forwarded-Access-Token"]
            }),
            ...options?.headers
        }
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const getRunKey = (params?: GetRunParams)=>{
    return [
        "/api/genie/runs/{run_id}",
        params
    ] as const;
};
export function useGetRun<TData = {
    data: PipelineRun;
}>(options: {
    params: GetRunParams;
    query?: Omit<UseQueryOptions<{
        data: PipelineRun;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: getRunKey(options.params),
        queryFn: ()=>getRun(options.params),
        ...options?.query
    });
}
export function useGetRunSuspense<TData = {
    data: PipelineRun;
}>(options: {
    params: GetRunParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: PipelineRun;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: getRunKey(options.params),
        queryFn: ()=>getRun(options.params),
        ...options?.query
    });
}
export interface ApplyOptimizationParams {
    run_id: string;
    "X-Forwarded-Host"?: string | null;
    "X-Forwarded-Preferred-Username"?: string | null;
    "X-Forwarded-User"?: string | null;
    "X-Forwarded-Email"?: string | null;
    "X-Request-Id"?: string | null;
    "X-Forwarded-Access-Token"?: string | null;
}
export const applyOptimization = async (params: ApplyOptimizationParams, options?: RequestInit): Promise<{
    data: ActionResponse;
}> =>{
    const res = await fetch(`/api/genie/runs/${params.run_id}/apply`, {
        ...options,
        method: "POST",
        headers: {
            ...(params?.["X-Forwarded-Host"] != null && {
                "X-Forwarded-Host": params["X-Forwarded-Host"]
            }),
            ...(params?.["X-Forwarded-Preferred-Username"] != null && {
                "X-Forwarded-Preferred-Username": params["X-Forwarded-Preferred-Username"]
            }),
            ...(params?.["X-Forwarded-User"] != null && {
                "X-Forwarded-User": params["X-Forwarded-User"]
            }),
            ...(params?.["X-Forwarded-Email"] != null && {
                "X-Forwarded-Email": params["X-Forwarded-Email"]
            }),
            ...(params?.["X-Request-Id"] != null && {
                "X-Request-Id": params["X-Request-Id"]
            }),
            ...(params?.["X-Forwarded-Access-Token"] != null && {
                "X-Forwarded-Access-Token": params["X-Forwarded-Access-Token"]
            }),
            ...options?.headers
        }
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useApplyOptimization(options?: {
    mutation?: UseMutationOptions<{
        data: ActionResponse;
    }, ApiError, {
        params: ApplyOptimizationParams;
    }>;
}) {
    return useMutation({
        mutationFn: (vars)=>applyOptimization(vars.params),
        ...options?.mutation
    });
}
export interface GetAsiResultsParams {
    run_id: string;
    iteration?: number | null;
}
export const getAsiResults = async (params: GetAsiResultsParams, options?: RequestInit): Promise<{
    data: AsiSummary;
}> =>{
    const searchParams = new URLSearchParams();
    if (params?.iteration != null) searchParams.set("iteration", String(params?.iteration));
    const queryString = searchParams.toString();
    const url = queryString ? `/api/genie/runs/${params.run_id}/asi-results?${queryString}` : `/api/genie/runs/${params.run_id}/asi-results`;
    const res = await fetch(url, {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const getAsiResultsKey = (params?: GetAsiResultsParams)=>{
    return [
        "/api/genie/runs/{run_id}/asi-results",
        params
    ] as const;
};
export function useGetAsiResults<TData = {
    data: AsiSummary;
}>(options: {
    params: GetAsiResultsParams;
    query?: Omit<UseQueryOptions<{
        data: AsiSummary;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: getAsiResultsKey(options.params),
        queryFn: ()=>getAsiResults(options.params),
        ...options?.query
    });
}
export function useGetAsiResultsSuspense<TData = {
    data: AsiSummary;
}>(options: {
    params: GetAsiResultsParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: AsiSummary;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: getAsiResultsKey(options.params),
        queryFn: ()=>getAsiResults(options.params),
        ...options?.query
    });
}
export interface GetComparisonParams {
    run_id: string;
    "X-Forwarded-Host"?: string | null;
    "X-Forwarded-Preferred-Username"?: string | null;
    "X-Forwarded-User"?: string | null;
    "X-Forwarded-Email"?: string | null;
    "X-Request-Id"?: string | null;
    "X-Forwarded-Access-Token"?: string | null;
}
export const getComparison = async (params: GetComparisonParams, options?: RequestInit): Promise<{
    data: ComparisonData;
}> =>{
    const res = await fetch(`/api/genie/runs/${params.run_id}/comparison`, {
        ...options,
        method: "GET",
        headers: {
            ...(params?.["X-Forwarded-Host"] != null && {
                "X-Forwarded-Host": params["X-Forwarded-Host"]
            }),
            ...(params?.["X-Forwarded-Preferred-Username"] != null && {
                "X-Forwarded-Preferred-Username": params["X-Forwarded-Preferred-Username"]
            }),
            ...(params?.["X-Forwarded-User"] != null && {
                "X-Forwarded-User": params["X-Forwarded-User"]
            }),
            ...(params?.["X-Forwarded-Email"] != null && {
                "X-Forwarded-Email": params["X-Forwarded-Email"]
            }),
            ...(params?.["X-Request-Id"] != null && {
                "X-Request-Id": params["X-Request-Id"]
            }),
            ...(params?.["X-Forwarded-Access-Token"] != null && {
                "X-Forwarded-Access-Token": params["X-Forwarded-Access-Token"]
            }),
            ...options?.headers
        }
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const getComparisonKey = (params?: GetComparisonParams)=>{
    return [
        "/api/genie/runs/{run_id}/comparison",
        params
    ] as const;
};
export function useGetComparison<TData = {
    data: ComparisonData;
}>(options: {
    params: GetComparisonParams;
    query?: Omit<UseQueryOptions<{
        data: ComparisonData;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: getComparisonKey(options.params),
        queryFn: ()=>getComparison(options.params),
        ...options?.query
    });
}
export function useGetComparisonSuspense<TData = {
    data: ComparisonData;
}>(options: {
    params: GetComparisonParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: ComparisonData;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: getComparisonKey(options.params),
        queryFn: ()=>getComparison(options.params),
        ...options?.query
    });
}
export interface DiscardOptimizationParams {
    run_id: string;
    "X-Forwarded-Host"?: string | null;
    "X-Forwarded-Preferred-Username"?: string | null;
    "X-Forwarded-User"?: string | null;
    "X-Forwarded-Email"?: string | null;
    "X-Request-Id"?: string | null;
    "X-Forwarded-Access-Token"?: string | null;
}
export const discardOptimization = async (params: DiscardOptimizationParams, options?: RequestInit): Promise<{
    data: ActionResponse;
}> =>{
    const res = await fetch(`/api/genie/runs/${params.run_id}/discard`, {
        ...options,
        method: "POST",
        headers: {
            ...(params?.["X-Forwarded-Host"] != null && {
                "X-Forwarded-Host": params["X-Forwarded-Host"]
            }),
            ...(params?.["X-Forwarded-Preferred-Username"] != null && {
                "X-Forwarded-Preferred-Username": params["X-Forwarded-Preferred-Username"]
            }),
            ...(params?.["X-Forwarded-User"] != null && {
                "X-Forwarded-User": params["X-Forwarded-User"]
            }),
            ...(params?.["X-Forwarded-Email"] != null && {
                "X-Forwarded-Email": params["X-Forwarded-Email"]
            }),
            ...(params?.["X-Request-Id"] != null && {
                "X-Request-Id": params["X-Request-Id"]
            }),
            ...(params?.["X-Forwarded-Access-Token"] != null && {
                "X-Forwarded-Access-Token": params["X-Forwarded-Access-Token"]
            }),
            ...options?.headers
        }
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useDiscardOptimization(options?: {
    mutation?: UseMutationOptions<{
        data: ActionResponse;
    }, ApiError, {
        params: DiscardOptimizationParams;
    }>;
}) {
    return useMutation({
        mutationFn: (vars)=>discardOptimization(vars.params),
        ...options?.mutation
    });
}
export interface GetIterationDetailParams {
    run_id: string;
}
export const getIterationDetail = async (params: GetIterationDetailParams, options?: RequestInit): Promise<{
    data: IterationDetailResponse;
}> =>{
    const res = await fetch(`/api/genie/runs/${params.run_id}/iteration-detail`, {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const getIterationDetailKey = (params?: GetIterationDetailParams)=>{
    return [
        "/api/genie/runs/{run_id}/iteration-detail",
        params
    ] as const;
};
export function useGetIterationDetail<TData = {
    data: IterationDetailResponse;
}>(options: {
    params: GetIterationDetailParams;
    query?: Omit<UseQueryOptions<{
        data: IterationDetailResponse;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: getIterationDetailKey(options.params),
        queryFn: ()=>getIterationDetail(options.params),
        ...options?.query
    });
}
export function useGetIterationDetailSuspense<TData = {
    data: IterationDetailResponse;
}>(options: {
    params: GetIterationDetailParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: IterationDetailResponse;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: getIterationDetailKey(options.params),
        queryFn: ()=>getIterationDetail(options.params),
        ...options?.query
    });
}
export interface GetIterationsParams {
    run_id: string;
}
export const getIterations = async (params: GetIterationsParams, options?: RequestInit): Promise<{
    data: IterationSummary[];
}> =>{
    const res = await fetch(`/api/genie/runs/${params.run_id}/iterations`, {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const getIterationsKey = (params?: GetIterationsParams)=>{
    return [
        "/api/genie/runs/{run_id}/iterations",
        params
    ] as const;
};
export function useGetIterations<TData = {
    data: IterationSummary[];
}>(options: {
    params: GetIterationsParams;
    query?: Omit<UseQueryOptions<{
        data: IterationSummary[];
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: getIterationsKey(options.params),
        queryFn: ()=>getIterations(options.params),
        ...options?.query
    });
}
export function useGetIterationsSuspense<TData = {
    data: IterationSummary[];
}>(options: {
    params: GetIterationsParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: IterationSummary[];
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: getIterationsKey(options.params),
        queryFn: ()=>getIterations(options.params),
        ...options?.query
    });
}
export interface GetProvenanceParams {
    run_id: string;
    iteration?: number | null;
    lever?: number | null;
}
export const getProvenance = async (params: GetProvenanceParams, options?: RequestInit): Promise<{
    data: ProvenanceSummary[];
}> =>{
    const searchParams = new URLSearchParams();
    if (params?.iteration != null) searchParams.set("iteration", String(params?.iteration));
    if (params?.lever != null) searchParams.set("lever", String(params?.lever));
    const queryString = searchParams.toString();
    const url = queryString ? `/api/genie/runs/${params.run_id}/provenance?${queryString}` : `/api/genie/runs/${params.run_id}/provenance`;
    const res = await fetch(url, {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const getProvenanceKey = (params?: GetProvenanceParams)=>{
    return [
        "/api/genie/runs/{run_id}/provenance",
        params
    ] as const;
};
export function useGetProvenance<TData = {
    data: ProvenanceSummary[];
}>(options: {
    params: GetProvenanceParams;
    query?: Omit<UseQueryOptions<{
        data: ProvenanceSummary[];
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: getProvenanceKey(options.params),
        queryFn: ()=>getProvenance(options.params),
        ...options?.query
    });
}
export function useGetProvenanceSuspense<TData = {
    data: ProvenanceSummary[];
}>(options: {
    params: GetProvenanceParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: ProvenanceSummary[];
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: getProvenanceKey(options.params),
        queryFn: ()=>getProvenance(options.params),
        ...options?.query
    });
}
export interface GetImprovementSuggestionsParams {
    run_id: string;
}
export const getImprovementSuggestions = async (params: GetImprovementSuggestionsParams, options?: RequestInit): Promise<{
    data: SuggestionOut[];
}> =>{
    const res = await fetch(`/api/genie/runs/${params.run_id}/suggestions`, {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const getImprovementSuggestionsKey = (params?: GetImprovementSuggestionsParams)=>{
    return [
        "/api/genie/runs/{run_id}/suggestions",
        params
    ] as const;
};
export function useGetImprovementSuggestions<TData = {
    data: SuggestionOut[];
}>(options: {
    params: GetImprovementSuggestionsParams;
    query?: Omit<UseQueryOptions<{
        data: SuggestionOut[];
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: getImprovementSuggestionsKey(options.params),
        queryFn: ()=>getImprovementSuggestions(options.params),
        ...options?.query
    });
}
export function useGetImprovementSuggestionsSuspense<TData = {
    data: SuggestionOut[];
}>(options: {
    params: GetImprovementSuggestionsParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: SuggestionOut[];
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: getImprovementSuggestionsKey(options.params),
        queryFn: ()=>getImprovementSuggestions(options.params),
        ...options?.query
    });
}
export interface GetPermissionDashboardParams {
    space_id?: string | null;
    metadata_only?: boolean;
    "X-Forwarded-Host"?: string | null;
    "X-Forwarded-Preferred-Username"?: string | null;
    "X-Forwarded-User"?: string | null;
    "X-Forwarded-Email"?: string | null;
    "X-Request-Id"?: string | null;
    "X-Forwarded-Access-Token"?: string | null;
}
export const getPermissionDashboard = async (params?: GetPermissionDashboardParams, options?: RequestInit): Promise<{
    data: PermissionDashboard;
}> =>{
    const searchParams = new URLSearchParams();
    if (params?.space_id != null) searchParams.set("space_id", String(params?.space_id));
    if (params?.metadata_only != null) searchParams.set("metadata_only", String(params?.metadata_only));
    const queryString = searchParams.toString();
    const url = queryString ? `/api/genie/settings/permissions?${queryString}` : "/api/genie/settings/permissions";
    const res = await fetch(url, {
        ...options,
        method: "GET",
        headers: {
            ...(params?.["X-Forwarded-Host"] != null && {
                "X-Forwarded-Host": params["X-Forwarded-Host"]
            }),
            ...(params?.["X-Forwarded-Preferred-Username"] != null && {
                "X-Forwarded-Preferred-Username": params["X-Forwarded-Preferred-Username"]
            }),
            ...(params?.["X-Forwarded-User"] != null && {
                "X-Forwarded-User": params["X-Forwarded-User"]
            }),
            ...(params?.["X-Forwarded-Email"] != null && {
                "X-Forwarded-Email": params["X-Forwarded-Email"]
            }),
            ...(params?.["X-Request-Id"] != null && {
                "X-Request-Id": params["X-Request-Id"]
            }),
            ...(params?.["X-Forwarded-Access-Token"] != null && {
                "X-Forwarded-Access-Token": params["X-Forwarded-Access-Token"]
            }),
            ...options?.headers
        }
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const getPermissionDashboardKey = (params?: GetPermissionDashboardParams)=>{
    return [
        "/api/genie/settings/permissions",
        params
    ] as const;
};
export function useGetPermissionDashboard<TData = {
    data: PermissionDashboard;
}>(options?: {
    params?: GetPermissionDashboardParams;
    query?: Omit<UseQueryOptions<{
        data: PermissionDashboard;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: getPermissionDashboardKey(options?.params),
        queryFn: ()=>getPermissionDashboard(options?.params),
        ...options?.query
    });
}
export function useGetPermissionDashboardSuspense<TData = {
    data: PermissionDashboard;
}>(options?: {
    params?: GetPermissionDashboardParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: PermissionDashboard;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: getPermissionDashboardKey(options?.params),
        queryFn: ()=>getPermissionDashboard(options?.params),
        ...options?.query
    });
}
export interface ListSpacesParams {
    "X-Forwarded-Host"?: string | null;
    "X-Forwarded-Preferred-Username"?: string | null;
    "X-Forwarded-User"?: string | null;
    "X-Forwarded-Email"?: string | null;
    "X-Request-Id"?: string | null;
    "X-Forwarded-Access-Token"?: string | null;
}
export const listSpaces = async (params?: ListSpacesParams, options?: RequestInit): Promise<{
    data: SpaceListResponse;
}> =>{
    const res = await fetch("/api/genie/spaces", {
        ...options,
        method: "GET",
        headers: {
            ...(params?.["X-Forwarded-Host"] != null && {
                "X-Forwarded-Host": params["X-Forwarded-Host"]
            }),
            ...(params?.["X-Forwarded-Preferred-Username"] != null && {
                "X-Forwarded-Preferred-Username": params["X-Forwarded-Preferred-Username"]
            }),
            ...(params?.["X-Forwarded-User"] != null && {
                "X-Forwarded-User": params["X-Forwarded-User"]
            }),
            ...(params?.["X-Forwarded-Email"] != null && {
                "X-Forwarded-Email": params["X-Forwarded-Email"]
            }),
            ...(params?.["X-Request-Id"] != null && {
                "X-Request-Id": params["X-Request-Id"]
            }),
            ...(params?.["X-Forwarded-Access-Token"] != null && {
                "X-Forwarded-Access-Token": params["X-Forwarded-Access-Token"]
            }),
            ...options?.headers
        }
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const listSpacesKey = (params?: ListSpacesParams)=>{
    return [
        "/api/genie/spaces",
        params
    ] as const;
};
export function useListSpaces<TData = {
    data: SpaceListResponse;
}>(options?: {
    params?: ListSpacesParams;
    query?: Omit<UseQueryOptions<{
        data: SpaceListResponse;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: listSpacesKey(options?.params),
        queryFn: ()=>listSpaces(options?.params),
        ...options?.query
    });
}
export function useListSpacesSuspense<TData = {
    data: SpaceListResponse;
}>(options?: {
    params?: ListSpacesParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: SpaceListResponse;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: listSpacesKey(options?.params),
        queryFn: ()=>listSpaces(options?.params),
        ...options?.query
    });
}
export interface CheckSpaceAccessParams {
    "X-Forwarded-Host"?: string | null;
    "X-Forwarded-Preferred-Username"?: string | null;
    "X-Forwarded-User"?: string | null;
    "X-Forwarded-Email"?: string | null;
    "X-Request-Id"?: string | null;
    "X-Forwarded-Access-Token"?: string | null;
}
export const checkSpaceAccess = async (data: CheckAccessRequest, params?: CheckSpaceAccessParams, options?: RequestInit): Promise<{
    data: AccessLevelEntry[];
}> =>{
    const res = await fetch("/api/genie/spaces/check-access", {
        ...options,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            ...(params?.["X-Forwarded-Host"] != null && {
                "X-Forwarded-Host": params["X-Forwarded-Host"]
            }),
            ...(params?.["X-Forwarded-Preferred-Username"] != null && {
                "X-Forwarded-Preferred-Username": params["X-Forwarded-Preferred-Username"]
            }),
            ...(params?.["X-Forwarded-User"] != null && {
                "X-Forwarded-User": params["X-Forwarded-User"]
            }),
            ...(params?.["X-Forwarded-Email"] != null && {
                "X-Forwarded-Email": params["X-Forwarded-Email"]
            }),
            ...(params?.["X-Request-Id"] != null && {
                "X-Request-Id": params["X-Request-Id"]
            }),
            ...(params?.["X-Forwarded-Access-Token"] != null && {
                "X-Forwarded-Access-Token": params["X-Forwarded-Access-Token"]
            }),
            ...options?.headers
        },
        body: JSON.stringify(data)
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useCheckSpaceAccess(options?: {
    mutation?: UseMutationOptions<{
        data: AccessLevelEntry[];
    }, ApiError, {
        params: CheckSpaceAccessParams;
        data: CheckAccessRequest;
    }>;
}) {
    return useMutation({
        mutationFn: (vars)=>checkSpaceAccess(vars.data, vars.params),
        ...options?.mutation
    });
}
export interface GetSpaceDetailParams {
    space_id: string;
    "X-Forwarded-Host"?: string | null;
    "X-Forwarded-Preferred-Username"?: string | null;
    "X-Forwarded-User"?: string | null;
    "X-Forwarded-Email"?: string | null;
    "X-Request-Id"?: string | null;
    "X-Forwarded-Access-Token"?: string | null;
}
export const getSpaceDetail = async (params: GetSpaceDetailParams, options?: RequestInit): Promise<{
    data: SpaceDetail;
}> =>{
    const res = await fetch(`/api/genie/spaces/${params.space_id}`, {
        ...options,
        method: "GET",
        headers: {
            ...(params?.["X-Forwarded-Host"] != null && {
                "X-Forwarded-Host": params["X-Forwarded-Host"]
            }),
            ...(params?.["X-Forwarded-Preferred-Username"] != null && {
                "X-Forwarded-Preferred-Username": params["X-Forwarded-Preferred-Username"]
            }),
            ...(params?.["X-Forwarded-User"] != null && {
                "X-Forwarded-User": params["X-Forwarded-User"]
            }),
            ...(params?.["X-Forwarded-Email"] != null && {
                "X-Forwarded-Email": params["X-Forwarded-Email"]
            }),
            ...(params?.["X-Request-Id"] != null && {
                "X-Request-Id": params["X-Request-Id"]
            }),
            ...(params?.["X-Forwarded-Access-Token"] != null && {
                "X-Forwarded-Access-Token": params["X-Forwarded-Access-Token"]
            }),
            ...options?.headers
        }
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const getSpaceDetailKey = (params?: GetSpaceDetailParams)=>{
    return [
        "/api/genie/spaces/{space_id}",
        params
    ] as const;
};
export function useGetSpaceDetail<TData = {
    data: SpaceDetail;
}>(options: {
    params: GetSpaceDetailParams;
    query?: Omit<UseQueryOptions<{
        data: SpaceDetail;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: getSpaceDetailKey(options.params),
        queryFn: ()=>getSpaceDetail(options.params),
        ...options?.query
    });
}
export function useGetSpaceDetailSuspense<TData = {
    data: SpaceDetail;
}>(options: {
    params: GetSpaceDetailParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: SpaceDetail;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: getSpaceDetailKey(options.params),
        queryFn: ()=>getSpaceDetail(options.params),
        ...options?.query
    });
}
export interface ApplySuggestionParams {
    suggestion_id: string;
}
export const applySuggestion = async (params: ApplySuggestionParams, options?: RequestInit): Promise<{
    data: SuggestionOut;
}> =>{
    const res = await fetch(`/api/genie/suggestions/${params.suggestion_id}/apply`, {
        ...options,
        method: "POST"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useApplySuggestion(options?: {
    mutation?: UseMutationOptions<{
        data: SuggestionOut;
    }, ApiError, {
        params: ApplySuggestionParams;
    }>;
}) {
    return useMutation({
        mutationFn: (vars)=>applySuggestion(vars.params),
        ...options?.mutation
    });
}
export interface ReviewSuggestionParams {
    suggestion_id: string;
    "X-Forwarded-Host"?: string | null;
    "X-Forwarded-Preferred-Username"?: string | null;
    "X-Forwarded-User"?: string | null;
    "X-Forwarded-Email"?: string | null;
    "X-Request-Id"?: string | null;
    "X-Forwarded-Access-Token"?: string | null;
}
export const reviewSuggestion = async (params: ReviewSuggestionParams, data: SuggestionReviewRequest, options?: RequestInit): Promise<{
    data: SuggestionOut;
}> =>{
    const res = await fetch(`/api/genie/suggestions/${params.suggestion_id}/review`, {
        ...options,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            ...(params?.["X-Forwarded-Host"] != null && {
                "X-Forwarded-Host": params["X-Forwarded-Host"]
            }),
            ...(params?.["X-Forwarded-Preferred-Username"] != null && {
                "X-Forwarded-Preferred-Username": params["X-Forwarded-Preferred-Username"]
            }),
            ...(params?.["X-Forwarded-User"] != null && {
                "X-Forwarded-User": params["X-Forwarded-User"]
            }),
            ...(params?.["X-Forwarded-Email"] != null && {
                "X-Forwarded-Email": params["X-Forwarded-Email"]
            }),
            ...(params?.["X-Request-Id"] != null && {
                "X-Request-Id": params["X-Request-Id"]
            }),
            ...(params?.["X-Forwarded-Access-Token"] != null && {
                "X-Forwarded-Access-Token": params["X-Forwarded-Access-Token"]
            }),
            ...options?.headers
        },
        body: JSON.stringify(data)
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useReviewSuggestion(options?: {
    mutation?: UseMutationOptions<{
        data: SuggestionOut;
    }, ApiError, {
        params: ReviewSuggestionParams;
        data: SuggestionReviewRequest;
    }>;
}) {
    return useMutation({
        mutationFn: (vars)=>reviewSuggestion(vars.params, vars.data),
        ...options?.mutation
    });
}
export interface TriggerOptimizationParams {
    "X-Forwarded-Host"?: string | null;
    "X-Forwarded-Preferred-Username"?: string | null;
    "X-Forwarded-User"?: string | null;
    "X-Forwarded-Email"?: string | null;
    "X-Request-Id"?: string | null;
    "X-Forwarded-Access-Token"?: string | null;
}
export const triggerOptimization = async (data: TriggerRequest, params?: TriggerOptimizationParams, options?: RequestInit): Promise<{
    data: TriggerResponse;
}> =>{
    const res = await fetch("/api/genie/trigger", {
        ...options,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            ...(params?.["X-Forwarded-Host"] != null && {
                "X-Forwarded-Host": params["X-Forwarded-Host"]
            }),
            ...(params?.["X-Forwarded-Preferred-Username"] != null && {
                "X-Forwarded-Preferred-Username": params["X-Forwarded-Preferred-Username"]
            }),
            ...(params?.["X-Forwarded-User"] != null && {
                "X-Forwarded-User": params["X-Forwarded-User"]
            }),
            ...(params?.["X-Forwarded-Email"] != null && {
                "X-Forwarded-Email": params["X-Forwarded-Email"]
            }),
            ...(params?.["X-Request-Id"] != null && {
                "X-Request-Id": params["X-Request-Id"]
            }),
            ...(params?.["X-Forwarded-Access-Token"] != null && {
                "X-Forwarded-Access-Token": params["X-Forwarded-Access-Token"]
            }),
            ...options?.headers
        },
        body: JSON.stringify(data)
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useTriggerOptimization(options?: {
    mutation?: UseMutationOptions<{
        data: TriggerResponse;
    }, ApiError, {
        params: TriggerOptimizationParams;
        data: TriggerRequest;
    }>;
}) {
    return useMutation({
        mutationFn: (vars)=>triggerOptimization(vars.data, vars.params),
        ...options?.mutation
    });
}
export interface GetTriggerStatusParams {
    run_id: string;
    "X-Forwarded-Host"?: string | null;
    "X-Forwarded-Preferred-Username"?: string | null;
    "X-Forwarded-User"?: string | null;
    "X-Forwarded-Email"?: string | null;
    "X-Request-Id"?: string | null;
    "X-Forwarded-Access-Token"?: string | null;
}
export const getTriggerStatus = async (params: GetTriggerStatusParams, options?: RequestInit): Promise<{
    data: RunStatusResponse;
}> =>{
    const res = await fetch(`/api/genie/trigger/status/${params.run_id}`, {
        ...options,
        method: "GET",
        headers: {
            ...(params?.["X-Forwarded-Host"] != null && {
                "X-Forwarded-Host": params["X-Forwarded-Host"]
            }),
            ...(params?.["X-Forwarded-Preferred-Username"] != null && {
                "X-Forwarded-Preferred-Username": params["X-Forwarded-Preferred-Username"]
            }),
            ...(params?.["X-Forwarded-User"] != null && {
                "X-Forwarded-User": params["X-Forwarded-User"]
            }),
            ...(params?.["X-Forwarded-Email"] != null && {
                "X-Forwarded-Email": params["X-Forwarded-Email"]
            }),
            ...(params?.["X-Request-Id"] != null && {
                "X-Request-Id": params["X-Request-Id"]
            }),
            ...(params?.["X-Forwarded-Access-Token"] != null && {
                "X-Forwarded-Access-Token": params["X-Forwarded-Access-Token"]
            }),
            ...options?.headers
        }
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const getTriggerStatusKey = (params?: GetTriggerStatusParams)=>{
    return [
        "/api/genie/trigger/status/{run_id}",
        params
    ] as const;
};
export function useGetTriggerStatus<TData = {
    data: RunStatusResponse;
}>(options: {
    params: GetTriggerStatusParams;
    query?: Omit<UseQueryOptions<{
        data: RunStatusResponse;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: getTriggerStatusKey(options.params),
        queryFn: ()=>getTriggerStatus(options.params),
        ...options?.query
    });
}
export function useGetTriggerStatusSuspense<TData = {
    data: RunStatusResponse;
}>(options: {
    params: GetTriggerStatusParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: RunStatusResponse;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: getTriggerStatusKey(options.params),
        queryFn: ()=>getTriggerStatus(options.params),
        ...options?.query
    });
}
export const version = async (options?: RequestInit): Promise<{
    data: VersionOut;
}> =>{
    const res = await fetch("/api/genie/version", {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const versionKey = ()=>{
    return [
        "/api/genie/version"
    ] as const;
};
export function useVersion<TData = {
    data: VersionOut;
}>(options?: {
    query?: Omit<UseQueryOptions<{
        data: VersionOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: versionKey(),
        queryFn: ()=>version(),
        ...options?.query
    });
}
export function useVersionSuspense<TData = {
    data: VersionOut;
}>(options?: {
    query?: Omit<UseSuspenseQueryOptions<{
        data: VersionOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: versionKey(),
        queryFn: ()=>version(),
        ...options?.query
    });
}
