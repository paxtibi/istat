        ??  ??                  v
  (   ??
 I S T A T                 <process>
  <connection id="istat-ddl">
    <properties>
      <property name="firebird.url" value="zdbc:firebird:/${database.path}" />
      <property name="firebird.user" value="${database.username}" />
      <property name="firebird.password" value="${database.password}" />
      <property name="firebird.dialect" value="${database.dialect}" />
      <property name="firebird.MaxConnections" value="database.MaxConnections}" />
      <property name="firebird.Wait" value="${database.Wait}" />
      <property name="firebird.PageSize" value="${database.PageSize}" />
      <property name="firebird.ForcedWrites" value="${database.ForcedWrites}" />
      <property name="firebird.MaxUnflushedWrites" value="{$database.MaxUnflushedWrites}" />
      <property name="transaction-type" value="" />
    </properties>
  </connection>
  <connection id="istat-dml">
    <properties>
      <property name="firebird.url" value="zdbc:firebird:/${database.path}" />
      <property name="firebird.user" value="${database.username}" />
      <property name="firebird.password" value="${database.password}" />
      <property name="firebird.dialect" value="${database.dialect}" />
      <property name="firebird.MaxConnections" value="database.MaxConnections}" />
      <property name="firebird.Wait" value="${database.Wait}" />
      <property name="firebird.PageSize" value="${database.PageSize}" />
      <property name="firebird.ForcedWrites" value="${database.ForcedWrites}" />
      <property name="firebird.MaxUnflushedWrites" value="{$database.MaxUnflushedWrites}" />
      <property name="transaction-type" value="tiRepeatableRead" />
    </properties>
  </connection>
  <item-writer id="istat-decessi-writer" class="TFirebirdDatabaseWriter" connection-ref="istat-dml" />
  <item-reader id="istat-decessi-reader" class="TFlatFileReader" file-name="${istat.decessi}" />
  <item-writer id="istat-popolazione-writer" class="TFirebirdDatabaseWriter" connection-ref="istat-dml" />
  <item-reader id="istat-popolazione-reader" class="TFlatFileReader" file-name="${istat.popolazione}" />
  <item-processor id="processor" class="TSplitCSVItemProcessor" field-sepparator="|" />
  <step id="step-decessi">
    <writer-ref ref="istat-decessi-writer" />
    <reader-ref ref="istat-decessi-reader" />
    <processor-ref ref="process" />
  </step>
  <step id="step-popolazione">
    <writer-ref ref="istat-popolazione-writer" />
    <reader-ref ref="istat-popolazione-reader" />
    <processor-ref ref="process" />
  </step>
  <task id="caricamento">
    <step-ref ref="step-decessi" />
    <step-ref ref="step-popolazione" />
  </task>
</process>