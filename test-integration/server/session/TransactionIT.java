/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.server.session;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.concept.Label;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Role;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.concept.type.Type;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.kb.concept.EntityTypeImpl;
import grakn.core.server.kb.structure.Shard;
import graql.lang.Graql;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlGet;
import graql.lang.query.GraqlInsert;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static grakn.core.util.GraqlTestUtil.assertCollectionsNonTriviallyEqual;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("CheckReturnValue")
public class TransactionIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
    private TransactionOLTP tx;
    private SessionImpl session;

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
        tx = session.transaction().write();
    }

    @After
    public void tearDown() {
        tx.close();
        session.close();
    }


    @Test
    public void whenGettingConceptById_ReturnTheConcept() {
        EntityType entityType = tx.putEntityType("test-name");
        assertEquals(entityType, tx.getConcept(entityType.id()));
    }

    @Test
    public void whenAttemptingToMutateViaTraversal_Throw() {
        expectedException.expect(VerificationException.class);
        expectedException.expectMessage("not read only");
        tx.getTinkerTraversal().V().drop().iterate();
    }

    @Test
    public void whenGettingAttributesByValue_ReturnTheMatchingAttributes() {
        String targetValue = "Geralt";
        assertTrue(tx.getAttributesByValue(targetValue).isEmpty());

        AttributeType<String> t1 = tx.putAttributeType("Parent 1", AttributeType.DataType.STRING);
        AttributeType<String> t2 = tx.putAttributeType("Parent 2", AttributeType.DataType.STRING);

        Attribute<String> r1 = t1.create(targetValue);
        Attribute<String> r2 = t2.create(targetValue);
        t2.create("Dragon");

        assertThat(tx.getAttributesByValue(targetValue), containsInAnyOrder(r1, r2));
    }

    @Test
    public void whenGettingTypesByName_ReturnTypes() {
        String entityTypeLabel = "My Entity Type";
        String relationTypeLabel = "My Relation Type";
        String roleTypeLabel = "My Role Type";
        String resourceTypeLabel = "My Attribute Type";
        String ruleTypeLabel = "My Rule Type";

        assertNull(tx.getEntityType(entityTypeLabel));
        assertNull(tx.getRelationType(relationTypeLabel));
        assertNull(tx.getRole(roleTypeLabel));
        assertNull(tx.getAttributeType(resourceTypeLabel));
        assertNull(tx.getRule(ruleTypeLabel));

        EntityType entityType = tx.putEntityType(entityTypeLabel);
        RelationType relationType = tx.putRelationType(relationTypeLabel);
        Role role = tx.putRole(roleTypeLabel);
        AttributeType attributeType = tx.putAttributeType(resourceTypeLabel, AttributeType.DataType.STRING);

        assertEquals(entityType, tx.getEntityType(entityTypeLabel));
        assertEquals(relationType, tx.getRelationType(relationTypeLabel));
        assertEquals(role, tx.getRole(roleTypeLabel));
        assertEquals(attributeType, tx.getAttributeType(resourceTypeLabel));
    }

    @Test
    public void whenGettingSubTypesFromRootMeta_IncludeAllTypes() {
        EntityType sampleEntityType = tx.putEntityType("Sample Entity Type");
        RelationType sampleRelationType = tx.putRelationType("Sample Relation Type");

        assertThat(tx.getMetaConcept().subs().collect(toSet()), containsInAnyOrder(
                tx.getMetaConcept(),
                tx.getMetaRelationType(),
                tx.getMetaEntityType(),
                tx.getMetaAttributeType(),
                sampleEntityType,
                sampleRelationType
        ));
    }

    @Test
    public void whenGettingTheShardingThreshold_TheCorrectValueIsReturned() {
        assertEquals(10000L, tx.shardingThreshold());
    }

    @Test
    public void whenBuildingAConceptFromAVertex_ReturnConcept() {
        EntityTypeImpl et = (EntityTypeImpl) tx.putEntityType("Sample Entity Type");
        assertEquals(et, tx.factory().buildConcept(et.vertex()));
    }

    @Test
    public void whenTransactionIsClosed_notUsable(){
        TransactionOLTP tx1 = session.transaction().write();
        tx1.close();
        expectedException.expect(TransactionException.class);
        expectedException.expectMessage("The transaction for keyspace [" + session.keyspace() + "] is closed.");
        SchemaConcept concept = tx1.getSchemaConcept(Label.of("thing"));
        assertEquals("thing", concept.label().toString());
    }

    @Test
    public void transactionRead_checkMutationsAllowedThrows(){
        TransactionOLTP tx1 = session.transaction().read();
        expectedException.expect(TransactionException.class);
        tx1.checkMutationAllowed();
        tx1.close();
        TransactionOLTP tx2 = session.transaction().write();
        tx2.checkMutationAllowed();
        tx2.close();
        TransactionOLTP tx3 = session.transaction().read();
        expectedException.expect(TransactionException.class);
        tx3.checkMutationAllowed();
        tx3.close();
    }

    @Test
    public void whenClosingATxWhichWasJustCommitted_DoNothing() {
        tx.commit();
        assertTrue("Transaction is still open after commit", tx.isClosed());
        tx.close();
        assertTrue("Transaction is somehow open after close", tx.isClosed());
    }

    @Test
    public void whenCommittingATxWhichWasJustCommitted_DoNothing() {
        tx.commit();
        assertTrue("Transaction is still open after commit", tx.isClosed());
        tx.commit();
        assertTrue("Transaction is somehow open after 2nd commit", tx.isClosed());
    }

    @Test
    public void whenAttemptingToMutateSchemaWithReadOnlyTransaction_ThrowOnCommit() {
        tx.close();
        String entityType = "My Entity Type";
        String roleType1 = "My Role Type 1";
        String relationType1 = "My Relation Type 1";

        //Fail Some Mutations
        tx = session.transaction().read();
        tx.putEntityType(entityType);
        expectedException.expectMessage(ErrorMessage.TRANSACTION_READ_ONLY.getMessage(tx.keyspace()));
        tx.commit();

        tx = session.transaction().read();
        tx.putRole(roleType1);
        expectedException.expectMessage(ErrorMessage.TRANSACTION_READ_ONLY.getMessage(tx.keyspace()));
        tx.commit();

        tx = session.transaction().read();
        tx.putRelationType(relationType1);
        expectedException.expectMessage(ErrorMessage.TRANSACTION_READ_ONLY.getMessage(tx.keyspace()));
        tx.commit();

    }

    @Test
    public void whenAttemptingToMutateInstancesWithReadOnlyTransaction_ThrowOnCommit() {
        tx.close();
        String entityType = "person";

        tx = session.transaction().write();
        tx.putEntityType(entityType);
        tx.commit();

        tx = session.transaction().read();
        EntityType person = tx.getEntityType("person");
        Entity human = person.create();
        expectedException.expectMessage(ErrorMessage.TRANSACTION_READ_ONLY.getMessage(tx.keyspace()));
        tx.commit();
    }

    @Test
    public void whenShardingSuperNode_EnsureNewInstancesGoToNewShard() {
        EntityTypeImpl entityType = (EntityTypeImpl) tx.putEntityType("The Special Type");
        Shard s1 = entityType.currentShard();

        //Add 3 instances to first shard
        Entity s1_e1 = entityType.create();
        Entity s1_e2 = entityType.create();
        Entity s1_e3 = entityType.create();
        tx.shard(entityType.id());

        Shard s2 = entityType.currentShard();

        //Add 5 instances to second shard
        Entity s2_e1 = entityType.create();
        Entity s2_e2 = entityType.create();
        Entity s2_e3 = entityType.create();
        Entity s2_e4 = entityType.create();
        Entity s2_e5 = entityType.create();

        tx.shard(entityType.id());
        Shard s3 = entityType.currentShard();

        //Add 2 instances to 3rd shard
        Entity s3_e1 = entityType.create();
        Entity s3_e2 = entityType.create();

        //Check Type was sharded correctly
        assertThat(entityType.shards().collect(toSet()), containsInAnyOrder(s1, s2, s3));

        //Check shards have correct instances
        assertThat(s1.links().collect(toSet()), containsInAnyOrder(s1_e1, s1_e2, s1_e3));
        assertThat(s2.links().collect(toSet()), containsInAnyOrder(s2_e1, s2_e2, s2_e3, s2_e4, s2_e5));
        assertThat(s3.links().collect(toSet()), containsInAnyOrder(s3_e1, s3_e2));
    }

    @Test
    public void whenCreatingAValidSchemaInSeparateThreads_EnsureValidationRulesHold() throws ExecutionException, InterruptedException {
        SessionImpl localSession = server.sessionWithNewKeyspace();

        ExecutorService executor = Executors.newCachedThreadPool();

        executor.submit(() -> {
            //Resources
            try (TransactionOLTP tx = localSession.transaction().write()) {
                AttributeType<Long> int_ = tx.putAttributeType("int", AttributeType.DataType.LONG);
                AttributeType<Long> foo = tx.putAttributeType("foo", AttributeType.DataType.LONG).sup(int_);
                tx.putAttributeType("bar", AttributeType.DataType.LONG).sup(int_);
                tx.putEntityType("FOO").has(foo);

                tx.commit();
            }
        }).get();

        //Relation Which Has Resources
        try (TransactionOLTP tx = localSession.transaction().write()) {
            tx.putEntityType("BAR").has(tx.getAttributeType("bar"));
            tx.commit();
        }
        localSession.close();
    }

    @Test
    public void whenShardingConcepts_EnsureCountsAreUpdated() {
        EntityType entity = tx.putEntityType("my amazing entity type");
        assertEquals(1L, tx.getShardCount(entity));

        tx.shard(entity.id());
        assertEquals(2L, tx.getShardCount(entity));
    }

    @Test
    public void whenGettingSupsOfASchemaConcept_ResultIncludesMetaThing() {
        EntityType yes = tx.putEntityType("yes");
        EntityType entity = tx.getMetaEntityType();
        Type thing = tx.getMetaConcept();
        Set<SchemaConcept> no = tx.sups(yes).collect(toSet());
        assertThat(no, containsInAnyOrder(yes, entity, thing));
        assertThat(tx.sups(entity).collect(toSet()), containsInAnyOrder(entity, thing));
        assertThat(tx.sups(thing).collect(toSet()), containsInAnyOrder(thing));
    }


    @Test
    public void insertAndDeleteRelationInSameTransaction_relationIsCorrectlyDeletedAndRolePlayersAreInserted(){
        tx.execute(Graql.parse("define person sub entity, plays friend; friendship sub relation, relates friend;").asDefine());
        tx.commit();
        tx = session.transaction().write();
        String relId = tx.execute(Graql.parse("insert $x isa person; $y isa person; $r (friend: $x, friend: $y) isa friendship;").asInsert()).get(0).get("r").id().getValue();
        tx.execute(Graql.parse("match $r id " + relId + "; delete $r;").asDelete());
        tx.commit();

        tx = session.transaction().write();
        List<ConceptMap> rolePlayersResult = tx.execute(Graql.parse("match $x isa person; get;").asGet());
        assertEquals(2, rolePlayersResult.size());
        List<ConceptMap> relationResult = tx.execute(Graql.parse("match $r id " + relId + "; get;").asGet());
        assertEquals(0, relationResult.size());
    }

    @Test
    public void insertAndDeleteSameRelationInDifferentTransactions_relationIsCorrectlyDeletedAndRolePlayersAreInserted(){
        tx.execute(Graql.parse("define person sub entity, plays friend; friendship sub relation, relates friend;").asDefine());
        tx.commit();
        tx = session.transaction().write();
        String relId = tx.execute(Graql.parse("insert $x isa person; $y isa person; $r (friend: $x, friend: $y) isa friendship;").asInsert()).get(0).get("r").id().getValue();
        tx.commit();
        tx = session.transaction().write();
        tx.execute(Graql.parse("match $r id " + relId + "; delete $r;").asDelete());
        tx.commit();


        tx = session.transaction().write();
        List<ConceptMap> rolePlayersResult = tx.execute(Graql.parse("match $x isa person; get;").asGet());
        assertEquals(2, rolePlayersResult.size());
        List<ConceptMap> relationResult = tx.execute(Graql.parse("match $r id " + relId + "; get;").asGet());
        assertEquals(0, relationResult.size());
    }

    @Test
    public void whenCommittingInferredConcepts_InferredConceptsAreNotPersisted(){
        tx.execute(Graql.<GraqlDefine>parse(
                    "define " +
                            "name sub attribute, datatype string;" +
                            "score sub attribute, datatype double;" +
                            "person sub entity, has name, has score;" +
                            "infer-attr sub rule," +
                            "when {" +
                            "  $p isa person, has score $s;" +
                            "  $s > 0.0;" +
                            "}, then {" +
                            "  $p has name 'Ganesh';" +
                            "};"
            ));
        tx.commit();

        tx = session.transaction().write();
        tx.execute(Graql.<GraqlInsert>parse("insert $p isa person, has score 10.0;"));
        tx.commit();

        tx = session.transaction().write();
        tx.execute(Graql.<GraqlGet>parse("match $p isa person, has name $n; get;"));
        tx.commit();

        tx = session.transaction().read();
        List<ConceptMap> answers = tx.execute(Graql.<GraqlGet>parse("match $p isa person, has name $n; get;"), false);
        assertTrue(answers.isEmpty());
    }

    @Test
    public void whenCommittingInferredAttributeEdge_EdgeIsNotPersisted(){
        tx.execute(Graql.<GraqlDefine>parse(
                "define " +
                        "score sub attribute, datatype double;" +
                        "person sub entity, has score;" +
                        "infer-attr sub rule," +
                        "when {" +
                        "  $p isa person, has score $s;" +
                        "  $q isa person; $q != $p;" +
                        "}, then {" +
                        "  $q has score $s;" +
                        "};"
        ));
        tx.commit();

        tx = session.transaction().write();
        tx.execute(Graql.<GraqlInsert>parse("insert $p isa person, has score 10.0;"));
        tx.execute(Graql.<GraqlInsert>parse("insert $q isa person;"));
        tx.commit();

        tx = session.transaction().write();
        List<ConceptMap> answers = tx.execute(Graql.<GraqlGet>parse("match $p isa person, has score $score; get;"));
        assertEquals(2, answers.size());
        tx.commit();

        tx = session.transaction().read();
        answers = tx.execute(Graql.<GraqlGet>parse("match $p isa person, has score $score; get;"), false);
        assertEquals(1, answers.size());
    }

    @Test
    public void whenCommittingConceptsDependentOnInferredConcepts_conceptsAndDependantsArePersisted(){
        String inferrableSchema = "define " +
                "baseEntity sub entity, has inferrableAttribute, has nonInferrableAttribute, plays someRole, plays anotherRole;" +
                "someEntity sub baseEntity;" +
                "nonInferrableAttribute sub attribute, datatype string;" +
                "inferrableAttribute sub attribute, datatype string, plays anotherRole;" +
                "inferrableRelation sub relation, has nonInferrableAttribute, relates someRole, relates anotherRole;" +

                "infer-attr sub rule," +
                "when { $p isa someEntity; not{$p has nonInferrableAttribute 'nonInferred';};}, " +
                "then { $p has inferrableAttribute 'inferred';};" +

                "infer-relation sub rule," +
                "when { $p isa someEntity; $q isa someEntity, has inferrableAttribute $r; $r 'inferred';}, " +
                "then { (someRole: $p, anotherRole: $r) isa inferrableRelation;};";

        tx.execute(Graql.<GraqlDefine>parse(inferrableSchema));

        tx.execute(Graql.<GraqlInsert>parse(
                "insert " +
                        "$p isa someEntity, has nonInferrableAttribute 'nonInferred';" +
                        "$q isa someEntity;"
        ));
        tx.commit();

        tx = session.transaction().write();
        List<ConceptMap> relationsWithInferredRolePlayer = tx.execute(Graql.<GraqlInsert>parse(
                "match " +
                        "$p isa someEntity;" +
                        "$q isa someEntity, has inferrableAttribute $r; $r 'inferred';" +
                        "insert " +
                        "$rel (someRole: $p, anotherRole: $r) isa inferrableRelation;" +
                        "$rel has nonInferrableAttribute 'relation with inferred roleplayer';"
        ));

        List<ConceptMap> inferredRelationWithAttributeAttached = tx.execute(Graql.<GraqlInsert>parse(
                "match " +
                        "$rel (someRole: $p, anotherRole: $r) isa inferrableRelation;" +
                        "insert " +
                        "$rel has nonInferrableAttribute 'inferred relation label';"
        ));
        tx.commit();

        tx = session.transaction().read();
        List<ConceptMap> relationsWithInferredRolePlayerPostCommit = tx.execute(Graql.parse(
                "match " +
                "$rel (someRole: $p, anotherRole: $r) isa inferrableRelation;" +
                "$rel has nonInferrableAttribute 'relation with inferred roleplayer'; get;")
                .asGet(), false);

        List<ConceptMap> inferredRelationWithAttributeAttachedPostCommit = tx.execute(Graql.parse(
                "match " +
                "$rel (someRole: $p, anotherRole: $r) isa inferrableRelation;" +
                "$rel has nonInferrableAttribute 'inferred relation label'; get $rel;")
                .asGet(), false);
        tx.close();

        assertCollectionsNonTriviallyEqual(relationsWithInferredRolePlayer, relationsWithInferredRolePlayerPostCommit);
        assertCollectionsNonTriviallyEqual(inferredRelationWithAttributeAttached, inferredRelationWithAttributeAttachedPostCommit);
    }

}
