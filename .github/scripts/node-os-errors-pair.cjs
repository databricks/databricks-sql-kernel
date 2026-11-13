'use strict';

const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { spawn } = require('node:child_process');
const assert = require('node:assert/strict');
const root = process.env.NODE_OS_RUN_ROOT;
const hash = file => crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
const slash = file => file.split(path.sep).join('/');
const read = file => JSON.parse(fs.readFileSync(file, 'utf8'));
const write = (file, value) => fs.writeFileSync(file, JSON.stringify(value, null, 2) + '\n');
const receipts = path.join(root, 'receipts');
const nativeFile = path.join(root, 'native', 'volume.node');
const code = value => typeof value === 'string' && /^[A-Z][A-Z0-9_]{0,39}$/.test(value) ? value : undefined;
const integer = value => Number.isInteger(value) && value >= -(2 ** 31) && value < 2 ** 31 ? value : undefined;
const classes = new Set(['Error', 'TypeError', 'StagingError', 'AuthenticationError', 'HiveDriverError', 'OperationStateError', 'ParameterError']);
const operations = new Set(['stat', 'fstat', 'open', 'realpath', 'write', 'fchmod', 'rename']);

// Only fields from the active owned path may cross the child boundary.
function errorFields(error, requested) {
  const metadata = error?.kernelMetadata;
  const cause = Object.getOwnPropertyDescriptor(error || {}, 'cause');
  const ownedMatch = error?.path === requested || error?.path === path.win32.normalize(requested);
  return {
    type: classes.has(error?.constructor?.name) ? error.constructor.name : 'Other',
    code: code(error?.code), errno: integer(error?.errno),
    path: ownedMatch ? error.path : undefined,
    pathMatches: ownedMatch,
    syscall: operations.has(error?.syscall) ? error.syscall : undefined,
    sqlState: typeof error?.sqlState === 'string' && /^[A-Z0-9]{5}$/.test(error.sqlState) ? error.sqlState : undefined,
    rawWin32: integer(metadata?.osErrorCode), nodeErrno: integer(metadata?.osErrorNodeErrno),
    rawFilename: metadata?.osErrorFilename === requested ? requested : undefined,
    rawOperation: operations.has(metadata?.osErrorOperation) ? metadata.osErrorOperation : undefined,
    transferCode: code(metadata?.errorCode), httpStatus: integer(metadata?.httpStatus),
    cause: cause ? { type: classes.has(cause.value?.constructor?.name) ? cause.value.constructor.name : 'Other',
      nonEnumerable: !cause.enumerable, bounded: cause.value instanceof Error &&
        typeof cause.value.message === 'string' && cause.value.message.length <= 500 } : undefined,
  };
}

function loadedIdentity(packageRoot, native) {
  const entry = path.join(packageRoot, 'dist', 'index.js');
  const router = path.join(packageRoot, 'native', 'kernel', 'index.js');
  const files = Object.keys(require.cache);
  const javascript = files.filter(file => file.startsWith(packageRoot + path.sep) && file.endsWith('.js'))
    .map(file => ({ file: slash(path.relative(packageRoot, file)), sha256: hash(file) }))
    .sort((a, b) => a.file.localeCompare(b.file));
  assert(files.includes(require.resolve(entry)) && javascript.length > 0);
  const sharedObjects = process.report.getReport().sharedObjects;
  const addon = native ? {
    file: 'native/volume.node', sha256: hash(nativeFile),
    inRequireCache: files.some(file => path.resolve(file).toLowerCase() === path.resolve(nativeFile).toLowerCase()),
    inSharedObjects: sharedObjects.some(file => path.resolve(file).toLowerCase() === path.resolve(nativeFile).toLowerCase()),
  } : undefined;
  if (native) {
    assert(files.includes(require.resolve(router)));
    assert(addon.inRequireCache && addon.inSharedObjects);
    assert.equal(addon.sha256, read(path.join(receipts, 'build.json')).addonSha256);
  }
  return { nodeVersion: process.version, architecture: process.arch, platform: process.platform,
    executableSha256: hash(process.execPath), packageVersion: read(path.join(packageRoot, 'package.json')).version,
    javascript, addon };
}

async function child(mode) {
  const cfg = read(0);
  const native = mode === 'native';
  const resultFile = path.join(receipts, mode + '.json');
  const out = { mode, ok: false, stage: 'preflight', orphanEvents: [], cleanup: {} };
  let active = false;
  let check = 'child-runtime';
  write(resultFile, out);
  let client, session, operation;
  let closing;
  function closeAll() {
    if (!closing) closing = (async () => {
      for (const [name, value] of [['operation', operation], ['session', session], ['client', client]]) {
        out.cleanup[name] = value ? 'pending' : 'not-created';
        write(resultFile, out);
        if (value) {
          try { await value.close(); out.cleanup[name] = 'closed'; }
          catch (error) { out.cleanup[name] = 'failed'; out.cleanupError = errorFields(error, cfg.requested); out.ok = false; }
        }
        write(resultFile, out);
      }
    })();
    return closing;
  }
  const unexpected = (error, kind) => {
    out.ok = false;
    out.unexpectedEvent = { kind, ...errorFields(error, cfg.requested) };
    write(resultFile, out);
    closeAll().finally(() => { write(resultFile, out); process.exit(1); });
  };
  process.on('uncaughtException', (error, origin) => {
    if (!native && active && origin === 'uncaughtException' && out.orphanEvents.length === 0 &&
        error?.code === 'ENOENT' && error.errno === out.control?.errno &&
        error.syscall === 'open' && errorFields(error, cfg.requested).pathMatches) {
      out.orphanEvents.push(errorFields(error, cfg.requested));
      write(resultFile, out);
    } else unexpected(error, 'uncaughtException');
  });
  process.on('unhandledRejection', error => unexpected(error, 'unhandledRejection'));
  try {
    assert.equal(process.platform, 'win32');
    assert.equal(process.arch, 'x64');
    assert.equal(process.version, 'v22.23.4');
    check = 'owned-missing-path';
    assert.equal(cfg.requested, slash(path.join(cfg.allowed, 'missing.bin')));
    assert(fs.statSync(cfg.allowed).isDirectory());
    assert(!fs.existsSync(cfg.requested));
    check = 'local-stat-control';
    try { fs.statSync(cfg.requested); } catch (error) { out.control = errorFields(error, cfg.requested); }
    assert.equal(out.control?.code, 'ENOENT');
    assert(out.control.errno < 0 && out.control.pathMatches && out.control.syscall === 'stat');
    const packageRoot = native ? path.join(root, 'node') : path.join(root, 'baseline', 'node_modules', '@databricks', 'sql');
    check = 'released-package-version';
    if (!native) assert.equal(read(path.join(packageRoot, 'package.json')).version, '3.0.0');
    check = 'native-override';
    assert.equal(process.env.NAPI_RS_NATIVE_LIBRARY_PATH, native ? nativeFile : undefined);
    check = 'public-client-load';
    const { DBSQLClient } = require(path.join(packageRoot, 'dist', 'index.js'));
    client = new DBSQLClient({ logger: { log() {}, getLevel: () => 'error' } });
    check = 'connect';
    out.stage = 'connect';
    write(resultFile, out);
    await client.connect({ host: process.env.E2E_HOST, path: process.env.E2E_PATH,
      token: process.env.E2E_ACCESS_TOKEN, useKernel: native });
    check = 'openSession';
    out.stage = 'openSession';
    write(resultFile, out);
    session = await client.openSession();
    check = 'put';
    out.stage = 'put';
    write(resultFile, out);
    active = true;
    const literal = text => "'" + text.replaceAll("'", "\\'") + "'";
    try {
      operation = await session.executeStatement(`PUT ${literal(cfg.requested)} INTO ${literal(cfg.remote)} OVERWRITE`,
        { runAsync: false, stagingAllowedLocalPath: [cfg.allowed] });
      out.stage = 'finished';
      write(resultFile, out);
      await operation.finished();
      out.stage = 'fetchAll';
      write(resultFile, out);
      await operation.fetchAll();
      out.unexpectedSuccess = true;
    } catch (error) {
      out.returned = errorFields(error, cfg.requested);
    }
    for (const field of ['code', 'errno', 'path', 'syscall']) {
      check = `returned-${field}`;
      assert.equal(out.returned?.[field], out.control[field]);
    }
    if (native) {
      check = 'native-error-class';
      assert.equal(out.returned.type, 'StagingError');
      check = 'native-raw-win32';
      assert(out.returned.rawWin32 > 0);
      check = 'native-translated-errno';
      assert.equal(out.returned.nodeErrno, out.control.errno);
      check = 'native-raw-filename';
      assert.equal(out.returned.rawFilename, cfg.requested);
      check = 'native-raw-operation';
      assert.equal(out.returned.rawOperation, 'stat');
      check = 'native-error-cause';
      assert(out.returned.cause?.bounded && out.returned.cause.nonEnumerable);
    }
    out.stage = 'matched-local-stat';
    out.ok = !out.unexpectedEvent;
  } catch (error) {
    out.ok = false;
    out.failedCheck = check;
    out.exitCode = 1;
    out.failure = errorFields(error, cfg.requested);
    out.failureKind = ['connect', 'openSession'].includes(out.stage) ? 'connection-or-authentication' :
      ['put', 'finished', 'fetchAll'].includes(out.stage) && out.returned?.code !== 'ENOENT' ? 'server-admission-or-transfer' : 'contract-or-provenance';
  } finally {
    write(resultFile, out);
    await closeAll();
    await new Promise(resolve => setImmediate(resolve));
    await new Promise(resolve => setImmediate(resolve));
    active = false;
    try {
      const packageRoot = native ? path.join(root, 'node') : path.join(root, 'baseline', 'node_modules', '@databricks', 'sql');
      out.identity = loadedIdentity(packageRoot, native);
    } catch { out.provenanceFailure = true; out.provenanceFailedCheck = 'loaded-identity'; out.ok = false; }
    write(resultFile, out);
  }
  process.exitCode = out.ok ? 0 : 1;
}

async function parent() {
  const pair = { ok: false, mode: 'live-missing-leaf-pair', children: [], cleanup: {} };
  const resultFile = path.join(receipts, 'pair.json');
  let owned;
  let check = 'pair-runtime';
  try {
    assert.equal(process.platform, 'win32');
    assert.equal(process.arch, 'x64');
    assert.equal(process.version, 'v22.23.4');
    check = 'pair-live-inputs';
    for (const key of ['E2E_HOST', 'E2E_PATH', 'E2E_ACCESS_TOKEN']) assert(process.env[key]);
    assert.equal(process.env.E2E_CATALOG, 'peco');
    assert.equal(process.env.E2E_SCHEMA, 'default');
    assert.equal(process.env.E2E_VOLUME, 'e2etests');
    check = 'pair-source-and-helper';
    const source = read(path.join(receipts, 'source.json'));
    assert(source.ok && source.nodePinVerified && source.helperSha256 === hash(__filename));
    check = 'pair-dependencies';
    assert(read(path.join(receipts, 'dependencies.json')).ok);
    check = 'pair-build';
    assert(read(path.join(receipts, 'build.json')).ok);
    pair.source = { H: source.H, Kprime: source.Kprime, N: source.N, helperSha256: source.helperSha256 };
    pair.dependencies = read(path.join(receipts, 'dependencies.json'));
    check = 'pair-owned-path';
    const guid = crypto.randomUUID();
    owned = path.join(root, 'paths', guid);
    const allowed = slash(path.join(owned, 'allowed'));
    fs.mkdirSync(allowed, { recursive: true });
    const cfg = { allowed, requested: slash(path.join(allowed, 'missing.bin')),
      remote: `/Volumes/peco/default/e2etests/node-os-${guid}.bin` };
    pair.owned = cfg;
    for (const mode of ['thrift', 'native']) {
      check = `${mode}-child`;
      const env = {};
      for (const key of ['SystemRoot', 'WINDIR', 'COMSPEC', 'PATH', 'PATHEXT', 'TEMP', 'TMP',
        'USERPROFILE', 'APPDATA', 'LOCALAPPDATA', 'NODE_EXTRA_CA_CERTS', 'E2E_HOST', 'E2E_PATH', 'E2E_ACCESS_TOKEN']) {
        const actual = Object.keys(process.env).find(name => name.toLowerCase() === key.toLowerCase());
        if (actual) env[actual] = process.env[actual];
      }
      env.NODE_OS_RUN_ROOT = root;
      if (mode === 'native') env.NAPI_RS_NATIVE_LIBRARY_PATH = nativeFile;
      const outcome = await new Promise(resolve => {
        const worker = spawn(process.execPath, [__filename, '--child', mode], { env, stdio: ['pipe', 'ignore', 'ignore'], windowsHide: true });
        let timedOut = false;
        const timer = setTimeout(() => { timedOut = true; worker.kill(); }, 180000);
        worker.on('error', () => { clearTimeout(timer); resolve({ spawnFailed: true }); });
        worker.on('close', (exitCode, signal) => { clearTimeout(timer); resolve({ exitCode, signal, timedOut }); });
        worker.stdin.on('error', () => {});
        worker.stdin.end(JSON.stringify(cfg));
      });
      const file = path.join(receipts, mode + '.json');
      pair.children.push({ mode, ...outcome, receipt: fs.existsSync(file) ? read(file) : undefined });
      write(resultFile, pair);
    }
    pair.ok = pair.children.length === 2 && pair.children.every(item => item.exitCode === 0 && !item.timedOut && item.receipt?.ok);
    if (!pair.ok) pair.failure = { check: 'pair-child-results', exitCode: 1 };
    if (pair.ok) {
      const [thrift, native] = pair.children.map(item => item.receipt);
      for (const field of ['code', 'errno', 'path', 'syscall']) {
        check = `pair-${field}`;
        assert.equal(thrift.returned[field], native.returned[field]);
      }
      check = 'pair-node-executable';
      assert.equal(thrift.identity.executableSha256, native.identity.executableSha256);
    }
  } catch { pair.ok = false; pair.preflightOrComparisonFailed = true; pair.failure = { check, exitCode: 1 }; }
  finally {
    if (owned) {
      try { fs.rmSync(owned, { recursive: true, force: true }); pair.cleanup.local = 'removed'; }
      catch { pair.cleanup.local = 'failed'; pair.ok = false; }
    }
    pair.cleanup.remote = 'no-remote-object-expected-from-missing-local-file';
    if (pair.children.some(item => item.receipt?.unexpectedSuccess)) pair.cleanup.remote = 'unexpected-PUT-completion-needs-investigation';
    write(resultFile, pair);
  }
  process.exitCode = pair.ok ? 0 : 1;
}

const args = process.argv.slice(2);
if (args.length === 0) parent().catch(() => { process.exitCode = 1; });
else if (args.length === 2 && args[0] === '--child' && ['thrift', 'native'].includes(args[1])) {
  child(args[1]).catch(() => { process.exitCode = 1; });
} else process.exitCode = 1;
