// Fills in the code blocks of the "Curating a successful run" section for one job capsule.
//
// The blocks are written for an example capsule. Its values are swapped for those of the capsule
// the reader enters, which are looked up in the job capsules Dandiset's public assets.jsonld and
// the capsule's own dataset_description.json. Both are served from S3, which allows any origin.
(() => {
  const SECTION_ID = "curating-a-successful-run";
  const ASSETS_URL = "https://dandiarchive.s3.amazonaws.com/dandisets/001697/draft/assets.jsonld";
  const JOB_ID_PATTERN = /job-\d{6}[0-9a-f]{6}(?:-\d+)?/;

  const EXAMPLE = {
    jobId: "job-260612eac7ed",
    stream: "block0_acquisition-ElectricalSeriesRaw_recording1",
    zarrId: "cc0f0a1e-f69e-489c-8501-255c20f83068",
    sourceUrl: "https://dandiarchive.s3.amazonaws.com/blobs/a05/ac4/a05ac4e6-030f-49b7-ac62-e1aa74e54dcb",
    outputPath: "sub-Pt03/sub-Pt03_desc-curated_ecephys.nwb",
    uploadDandisetId: "000397",
  };

  let assetsPromise = null;
  let resolved = null;

  const loadAssets = () => {
    assetsPromise ??= fetch(ASSETS_URL).then((response) => {
      if (!response.ok) throw new Error(`Could not load ${ASSETS_URL} (HTTP ${response.status}).`);
      return response.json();
    });
    assetsPromise.catch(() => (assetsPromise = null));
    return assetsPromise;
  };

  const s3Url = (asset, kind) => (asset.contentUrl || []).find((url) => url.includes(`amazonaws.com/${kind}/`));

  const blobUrl = (contentId) =>
    `https://dandiarchive.s3.amazonaws.com/blobs/${contentId.slice(0, 3)}/${contentId.slice(3, 6)}/${contentId}`;

  // Keeps the asset's entities and suffix, with its desc entity (if any) replaced by the given one.
  const curatedPath = (assetPath, desc) => {
    const slash = assetPath.lastIndexOf("/");
    const directory = assetPath.slice(0, slash + 1);
    const parts = assetPath.slice(slash + 1).replace(/\.nwb$/, "").split("_");
    const suffix = parts.length > 1 && !parts.at(-1).includes("-") ? parts.pop() : null;
    const entities = parts.filter((part) => !part.startsWith("desc-"));
    entities.push(`desc-${desc}`);
    if (suffix) entities.push(suffix);
    return `${directory}${entities.join("_")}.nwb`;
  };

  const findCapsule = (assets, text, jobId) => {
    const analyzers = new Map();
    for (const asset of assets) {
      const [capsulePath, analyzerName] = asset.path.split("/derivatives/postprocessed/");
      if (analyzerName === undefined || !capsulePath.endsWith(`/${jobId}`) || !analyzerName.endsWith(".zarr")) continue;
      const zarrUrl = s3Url(asset, "zarr");
      if (!zarrUrl) continue;
      if (!analyzers.has(capsulePath)) analyzers.set(capsulePath, []);
      analyzers.get(capsulePath).push({
        stream: analyzerName.replace(/\.zarr$/, ""),
        zarrId: zarrUrl.replace(/\/$/, "").split("/").at(-1),
      });
    }

    let capsulePaths = [...analyzers.keys()];
    if (capsulePaths.length > 1) {
      capsulePaths = capsulePaths.filter((path) => text.includes(path.replace(/^derivatives\//, "")));
    }
    if (capsulePaths.length === 1) {
      const capsulePath = capsulePaths[0];
      const streams = analyzers.get(capsulePath).sort((a, b) => a.stream.localeCompare(b.stream));
      return { capsulePath, streams };
    }
    if (analyzers.size > 1) {
      const matches = [...analyzers.keys()].join("\n");
      throw new Error(`${jobId} matches more than one capsule. Paste its full path instead.\n${matches}`);
    }
    if (assets.some((asset) => asset.path.includes(`/${jobId}/`))) {
      throw new Error(`${jobId} has no postprocessed outputs. Only successful aind+ephys capsules can be curated.`);
    }
    throw new Error(`${jobId} was not found in Dandiset 001697. Archived capsules are not successful.`);
  };

  const resolveCapsule = async (input) => {
    const text = decodeURIComponent(input.trim());
    const jobId = text.match(JOB_ID_PATTERN)?.[0];
    if (!jobId) throw new Error("Enter a job ID such as job-260612eac7ed, or a capsule path or link that contains one.");

    const assets = await loadAssets();
    const { capsulePath, streams } = findCapsule(assets, text, jobId);

    const description = assets.find((asset) => asset.path === `${capsulePath}/dataset_description.json`);
    const descriptionUrl = description && s3Url(description, "blobs");
    if (!descriptionUrl) throw new Error(`${capsulePath} has no dataset_description.json.`);
    const response = await fetch(descriptionUrl);
    if (!response.ok) throw new Error(`Could not load the dataset_description.json of ${jobId}.`);
    const provenance = (await response.json()).DandiCompute || {};
    const assetPath = provenance.within_dandiset_path || provenance.dandi_path;
    if (!provenance.dandiset_id || !assetPath || !provenance.content_id) {
      throw new Error(`The dataset_description.json of ${jobId} does not record its source asset.`);
    }

    return {
      jobId: capsulePath.split("/").at(-1),
      capsulePath,
      streams,
      dandisetId: provenance.dandiset_id,
      assetPath,
      sourceUrl: blobUrl(provenance.content_id),
    };
  };

  // Labels a stream by the parts of its name that the capsule's other streams do not share,
  // e.g. "ElectricalSeriesProbe00AP" for "block0_acquisition-ElectricalSeriesProbe00AP_recording1".
  const streamLabel = (streams, streamIndex) => {
    const tokenLists = streams.map(({ stream }) => stream.split(/[^A-Za-z0-9]+/).filter(Boolean));
    const label = tokenLists[streamIndex].filter((token) => !tokenLists.every((tokens) => tokens.includes(token)));
    return label.join("") || String(streamIndex);
  };

  const valuesFor = (capsule, streamIndex, uploadDandisetId) => {
    const { stream, zarrId } = capsule.streams[streamIndex];
    const desc = capsule.streams.length > 1 ? `curated${streamLabel(capsule.streams, streamIndex)}` : "curated";
    return {
      jobId: capsule.jobId,
      stream,
      zarrId,
      sourceUrl: capsule.sourceUrl,
      outputPath: curatedPath(capsule.assetPath, desc),
      uploadDandisetId,
    };
  };

  // One pass, longest first, so that neither an example value inside a longer one nor a value
  // already filled in is replaced again.
  const exampleKeys = Object.keys(EXAMPLE).sort((a, b) => EXAMPLE[b].length - EXAMPLE[a].length);
  const examplePattern = new RegExp(
    exampleKeys.map((key) => EXAMPLE[key].replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|"),
    "g",
  );
  const substitute = (template, values) =>
    template.replace(examplePattern, (match) => values[exampleKeys.find((key) => EXAMPLE[key] === match)]);

  const addCopyButton = (block) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "curation-copy";
    button.textContent = "Copy";
    button.addEventListener("click", async () => {
      await navigator.clipboard.writeText(block.querySelector("pre").textContent);
      button.textContent = "Copied";
      setTimeout(() => (button.textContent = "Copy"), 1500);
    });
    block.prepend(button);
  };

  const setUp = () => {
    const section = document.getElementById(SECTION_ID);
    const mount = document.getElementById("curation-widget");
    if (!section || !mount) return;

    const blocks = [...section.querySelectorAll("div.highlight")];
    for (const block of blocks) {
      const pre = block.querySelector("pre");
      pre.dataset.template = pre.textContent;
      block.classList.add("curation-code");
      addCopyButton(block);
    }

    mount.innerHTML = `
      <form class="curation-form">
        <label>Job capsule
          <input name="capsule" type="text" spellcheck="false" autocomplete="off"
                 placeholder="${EXAMPLE.jobId}, a capsule path, or a DANDI link to it">
        </label>
        <button type="submit">Fill in</button>
        <label class="curation-stream" hidden>Stream <select name="stream"></select></label>
        <label>Upload to Dandiset <input name="dandiset" type="text" spellcheck="false" autocomplete="off"></label>
      </form>
      <p class="curation-status" role="status">
        Showing <code>${EXAMPLE.jobId}</code>. Enter another capsule to fill in its values.
      </p>`;
    const form = mount.querySelector("form");
    const status = mount.querySelector(".curation-status");
    const streamField = mount.querySelector(".curation-stream");
    const streamSelect = form.elements.stream;
    const dandisetInput = form.elements.dandiset;
    dandisetInput.value = EXAMPLE.uploadDandisetId;

    const render = () => {
      const uploadDandisetId = dandisetInput.value.trim() || (resolved ?? EXAMPLE).dandisetId || EXAMPLE.uploadDandisetId;
      const values = resolved
        ? valuesFor(resolved, streamSelect.selectedIndex, uploadDandisetId)
        : { ...EXAMPLE, uploadDandisetId };
      for (const block of blocks) {
        const pre = block.querySelector("pre");
        pre.textContent = substitute(pre.dataset.template, values);
      }
    };

    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      status.classList.remove("curation-error");
      status.textContent = "Looking up the capsule in the job capsules index (about 9 MB on first use)…";
      try {
        resolved = await resolveCapsule(form.elements.capsule.value);
      } catch (error) {
        status.classList.add("curation-error");
        // fetch rejects with a TypeError when the request never completes.
        status.textContent =
          error instanceof TypeError
            ? `Could not reach the DANDI archive (${error.message}). Press Fill in to try again.`
            : error.message;
        return;
      }
      streamSelect.replaceChildren(...resolved.streams.map(({ stream }) => new Option(stream)));
      streamField.hidden = resolved.streams.length < 2;
      dandisetInput.value = resolved.dandisetId;
      render();
      status.innerHTML = "";
      status.append("Filled in for ");
      const code = document.createElement("code");
      code.textContent = resolved.capsulePath;
      status.append(code, ".");
    });
    streamSelect.addEventListener("change", render);
    dandisetInput.addEventListener("input", render);
  };

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", setUp);
  else setUp();
})();
