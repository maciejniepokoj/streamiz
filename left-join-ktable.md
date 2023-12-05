    <VR, KO, VO> KTable<K, VR> leftJoin(final KTable<KO, VO> other,
                                        final Function<V, KO> foreignKeyExtractor,
                                        final ValueJoiner<V, VO, VR> joiner,
                                        final TableJoined<K, KO> tableJoined,
                                        final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);


KTableImpl => doJoinOnForeignKey(...)

