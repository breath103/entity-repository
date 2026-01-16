export type EntityDefinitions = Record<string, Record<string, unknown>>;
export type EntityConfig<Definitions extends EntityDefinitions> = {
    [Table in keyof Definitions]: {
        id: keyof Definitions[Table] & string;
    };
};
export type RepositoryConfig<Definitions extends EntityDefinitions, Config extends EntityConfig<Definitions>> = {
    entities: Config;
};
export type EntityIdTuple<Definitions extends EntityDefinitions, Config extends EntityConfig<Definitions>, Table extends keyof Definitions> = Pick<Definitions[Table], Config[Table]["id"]>;
export type EntityEvent<Entity> = {
    timestamp: Date;
} & ({
    type: "insert";
    new: Entity;
} | {
    type: "update";
    old: Entity;
    new: Entity;
} | {
    type: "delete";
    old: Entity;
});
export type RepositoryQuery<Entity> = {
    entity: Entity | null;
} & ({
    status: "fetching";
} | {
    status: "idle";
} | {
    status: "error";
    error: Error;
});
//# sourceMappingURL=types.d.ts.map