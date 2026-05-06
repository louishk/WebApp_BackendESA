import React, { useState, useMemo, useRef, useEffect, useCallback } from "react";
import {
  Wind, Sun, Wine, Droplets, ShieldCheck, Star, Snowflake,
  MapPin, Phone, Clock, Search, Calendar, MessageCircle,
  ChevronRight, CheckCircle2, X, SlidersHorizontal, Tag,
  Zap, MapPinned, DollarSign, Ticket, AlertCircle, Filter,
  ChevronDown, ChevronUp, Camera, Upload, Package, Home,
  Briefcase, Archive, FileText, Sofa, Box, ArrowRight,
  ArrowLeft, Sparkles, Info, Check, Loader2, Globe,
  Thermometer, TrendingDown, CreditCard, Shield, Lock,
  CalendarCheck, UserCheck, Dumbbell, Cpu, Palette,
  BookOpen, Music, Plus, Trash2, Pencil, Send, Train, Bus,
  Gift, HelpCircle, CircleDot, MessageSquare
} from "lucide-react";

const N = "#1B2A4A";
const R = "#E63946";
const BG = "#F4F5F7";
const BD = "#E8EAF0";
const MT = "#8B92A5";
const GR = "#16a34a";
const AM = "#d97706";
const BL = "#3b82f6";
const FT = '"Outfit",system-ui,sans-serif';
const SH = "0 1px 4px rgba(27,42,74,0.06)";
const RAD = 14;

const FAC = {
  angMoKio: { name: "Ang Mo Kio", addr: "12 Ang Mo Kio Industrial Park 2", postal: "SG 569500", units: 42, rating: 4.7, reviews: 234, region: "SG", mrt: "Ang Mo Kio MRT — 8 min", bus: "24,73,133" },
  tai_seng: { name: "Tai Seng", addr: "5 Tai Seng Drive", postal: "SG 535215", units: 38, rating: 4.6, reviews: 187, region: "SG", mrt: "Tai Seng MRT — 4 min", bus: "22,24,43" },
  kallang: { name: "Kallang Way", addr: "1 Kallang Way 2A", postal: "SG 347495", units: 31, rating: 4.8, reviews: 312, region: "SG", mrt: "Mountbatten MRT — 6 min", bus: "61,63,158" },
  alexandra: { name: "Alexandra", addr: "63 Alexandra Terrace", postal: "SG 119937", units: 27, rating: 4.5, reviews: 156, region: "SG", mrt: "Redhill MRT — 10 min", bus: "51,145,195" },
  changi: { name: "Changi", addr: "9 Changi South Street 3", postal: "SG 486361", units: 35, rating: 4.7, reviews: 201, region: "SG", mrt: "Expo MRT — 12 min", bus: "9,19,89" },
};

const UNITS = [
  { id: "A1088", sz: "S", dims: "5×5", sqft: 25, tags: ["Near lift","Near MRT"], price: 180, clim: "AC", fac: "angMoKio" },
  { id: "B2374", sz: "S", dims: "5×7", sqft: 28, tags: ["Near MRT"], price: 210, clim: "None", fac: "angMoKio" },
  { id: "C4921", sz: "M", dims: "8×10", sqft: 55, tags: ["Ground Floor"], price: 450, clim: "Refrig", fac: "angMoKio" },
  { id: "D8650", sz: "S", dims: "4×4", sqft: 16, tags: ["Near lift"], price: 95, clim: "DC", fac: "angMoKio" },
  { id: "E1093", sz: "L", dims: "10×12", sqft: 80, tags: ["Wide Corridor"], price: 620, clim: "AC+DC", fac: "angMoKio" },
  { id: "F7248", sz: "S", dims: "5×5", sqft: 25, tags: ["Near MRT"], price: 185, clim: "AC", fac: "angMoKio" },
  { id: "TS101", sz: "S", dims: "5×5", sqft: 25, tags: ["Near MRT"], price: 155, clim: "AC", fac: "tai_seng" },
  { id: "TS202", sz: "M", dims: "7×8", sqft: 50, tags: ["Ground Floor"], price: 340, clim: "None", fac: "tai_seng" },
  { id: "KW301", sz: "S", dims: "4×5", sqft: 20, tags: ["Near lift"], price: 78, clim: "AC", fac: "kallang" },
  { id: "KW402", sz: "L", dims: "9×10", sqft: 85, tags: ["Drive-up"], price: 520, clim: "AC+DC", fac: "kallang" },
  { id: "AX101", sz: "M", dims: "6×8", sqft: 45, tags: ["Near lift"], price: 310, clim: "AC", fac: "alexandra" },
  { id: "CH201", sz: "XL", dims: "12×15", sqft: 120, tags: ["Drive-up"], price: 750, clim: "None", fac: "changi" },
  { id: "CH302", sz: "M", dims: "8×8", sqft: 55, tags: ["Near lift"], price: 290, clim: "AC", fac: "changi" },
];

const DSC = [
  { id: 1, name: "1st Month Free", pct: 0, type: "first_free", badge: "POPULAR", durs: [3,6,12] },
  { id: 2, name: "20% Off 6mo", pct: 0.2, type: "percent", durs: [6] },
  { id: 3, name: "30% Off 12mo", pct: 0.3, type: "percent", badge: "BEST", durs: [12] },
  { id: 4, name: "10% Student", pct: 0.1, type: "percent", durs: [1,3,6,12] },
];

const CLM = [
  { id: "None", icon: Sun, short: "None", purs: ["household","furniture","moving"] },
  { id: "AC", icon: Wind, short: "AC", purs: ["business","personal","sports"] },
  { id: "DC", icon: Droplets, short: "DC", purs: ["documents","books"] },
  { id: "AC+DC", icon: ShieldCheck, short: "AC+DC", purs: ["sensitive_docs","electronics","art","instruments","commercial"] },
  { id: "Refrig", icon: Snowflake, short: "Refrig", purs: ["wine"] },
];

const SZS = [{ l: "All", r: [0,999] },{ l: "S", sub: "0-30", r: [0,30] },{ l: "M", sub: "30-60", r: [30,60] },{ l: "L", sub: "60-90", r: [60,90] },{ l: "XL", sub: "90+", r: [90,999] }];
const toSz = sq => sq <= 30 ? "S" : sq <= 60 ? "M" : sq <= 90 ? "L" : "XL";

const PUR = [
  { id: "household", label: "Household", icon: Home, sq: 50, cl: "None" },
  { id: "business", label: "Business", icon: Briefcase, sq: 30, cl: "AC" },
  { id: "wine", label: "Wine", icon: Wine, sq: 20, cl: "Refrig" },
  { id: "documents", label: "Documents", icon: FileText, sq: 12, cl: "DC" },
  { id: "sensitive_docs", label: "Sensitive Docs", icon: Shield, sq: 15, cl: "AC+DC" },
  { id: "furniture", label: "Furniture", icon: Sofa, sq: 80, cl: "None" },
  { id: "moving", label: "Moving", icon: Package, sq: 120, cl: "None" },
  { id: "personal", label: "Personal", icon: Box, sq: 15, cl: "AC" },
  { id: "commercial", label: "Commercial", icon: Archive, sq: 100, cl: "AC+DC" },
  { id: "sports", label: "Sports", icon: Dumbbell, sq: 25, cl: "AC" },
  { id: "electronics", label: "Electronics", icon: Cpu, sq: 20, cl: "AC+DC" },
  { id: "art", label: "Art", icon: Palette, sq: 30, cl: "AC+DC" },
  { id: "books", label: "Books", icon: BookOpen, sq: 18, cl: "DC" },
  { id: "instruments", label: "Music", icon: Music, sq: 25, cl: "AC+DC" },
];

const AI_POOL = [
  { items: "Sofa (3-seater)", sq: 18 },{ items: "Coffee table", sq: 4 },{ items: "King bed", sq: 22 },
  { items: "Wardrobe", sq: 12 },{ items: "Office desk", sq: 10 },{ items: "Boxes (×6)", sq: 8 },
  { items: "Wine cases (×10)", sq: 18 },{ items: "Bicycle", sq: 6 },{ items: "Piano", sq: 16 },
  { items: "Art canvases (×8)", sq: 10 },{ items: "File boxes (×10)", sq: 12 },{ items: "Suitcases (×3)", sq: 6 },
];

const INS = [{ id: "none", name: "None", p: 0 },{ id: "basic", name: "Basic ($5k)", p: 12 },{ id: "standard", name: "Standard ($15k)", p: 22, badge: "REC" },{ id: "premium", name: "Premium ($50k)", p: 45 }];
const DURS = [{ m: 1, l: "1mo" },{ m: 3, l: "3mo" },{ m: 6, l: "6mo" },{ m: 12, l: "12mo" }];
const COUPS = { SAVE10: 0.1, EXTRA15: 0.15, WELCOME: 0.05 };

/*
  CHECKOUT QUESTIONS — config-driven conditional qualifier module
  
  Each question has:
  - id: unique key
  - question: display text
  - type: "single" (radio) | "multi" (checkbox) | "text" (free input)
  - options: array of {id, label, desc?} for single/multi
  - required: boolean
  - active: boolean — master toggle (backend can flip this)
  - trigger: function(context) => boolean — contextual trigger
    context = { disc, unit, dur, facility }
  - icon: lucide icon component
  - badge: optional label shown on the card
*/
var CHECKOUT_QUESTIONS = [
  {
    id: "voucher_pref",
    question: "Which voucher would you prefer?",
    type: "single",
    options: [
      { id: "ntuc", label: "NTUC FairPrice $20", desc: "Grocery voucher" },
      { id: "grab", label: "Grab $20", desc: "Transport or food" },
      { id: "capita", label: "CapitaLand $20", desc: "Shopping voucher" },
      { id: "none", label: "No thanks", desc: "Skip the voucher" },
    ],
    required: true,
    active: true,
    trigger: function(ctx) { return ctx.disc && ctx.disc.type === "first_free"; },
    icon: Gift,
    badge: "PROMO GIFT",
  },
  {
    id: "floor_pref",
    question: "Do you have a floor preference?",
    type: "single",
    options: [
      { id: "ground", label: "Ground floor", desc: "Easy loading access" },
      { id: "upper", label: "Upper floor", desc: "Often more availability" },
      { id: "any", label: "No preference" },
    ],
    required: false,
    active: true,
    trigger: function(ctx) { return ctx.unit && ctx.unit.sqft >= 50; },
    icon: HelpCircle,
  },
  {
    id: "movein_help",
    question: "Need help on move-in day?",
    type: "multi",
    options: [
      { id: "trolley", label: "Trolley / dolly", desc: "Free to use on-site" },
      { id: "movers", label: "Professional movers", desc: "We'll send you a quote" },
      { id: "packing", label: "Packing supplies", desc: "Boxes, tape, bubble wrap" },
    ],
    required: false,
    active: true,
    trigger: function() { return true; },
    icon: Package,
  },
  {
    id: "referral",
    question: "How did you hear about us?",
    type: "single",
    options: [
      { id: "google", label: "Google search" },
      { id: "social", label: "Social media" },
      { id: "friend", label: "Friend / referral" },
      { id: "walk_in", label: "Walked past" },
      { id: "other", label: "Other" },
    ],
    required: false,
    active: true,
    trigger: function() { return true; },
    icon: MessageSquare,
  },
  {
    id: "long_term_interest",
    question: "Interested in our loyalty programme?",
    type: "single",
    options: [
      { id: "yes", label: "Yes, tell me more" },
      { id: "no", label: "Not right now" },
    ],
    required: false,
    active: true,
    trigger: function(ctx) { return ctx.dur >= 6; },
    icon: Star,
  },
  {
    id: "special_request",
    question: "Any special requests?",
    type: "text",
    options: [],
    required: false,
    active: false,
    trigger: function() { return true; },
    icon: MessageSquare,
  },
];

const fmtD = d => d ? new Date(d).toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" }) : "";
const dimM = (y, m) => new Date(y, m + 1, 0).getDate();
const addMo = (d, n) => { const x = new Date(d); x.setMonth(x.getMonth() + n); return x; };

function calcBill(moveIn, dur, price, ins, region, disc, coupon) {
  if (!moveIn) return null;
  const d = new Date(moveIn);
  const dm = dimM(d.getFullYear(), d.getMonth());
  const rem = dm - d.getDate() + 1;
  const first = price * (rem / dm);
  const ongoing = dur - 1;
  const subseq = price * ongoing;
  let ff = 0, pd = 0;
  if (disc && disc.type === "first_free") ff = first;
  if (disc && disc.type === "percent") pd = (first + subseq) * disc.pct;
  const af = first + subseq - ff - pd;
  const ca = coupon && coupon.valid ? af * coupon.pct : 0;
  const ac = af - ca;
  const ic = (INS.find(i => i.id === ins) || { p: 0 }).p * dur;
  const pt = ac + ic;
  const tx = region === "MY" ? { l: "SST", r: 0.06 } : { l: "GST", r: 0.09 };
  const ta = pt * tx.r;
  return {
    first, ongoing, subseq, ff, pd, discN: disc ? disc.name : null,
    ca, cCode: coupon ? coupon.code : null, ac, ic, pt, ta, txL: tx.l, txR: tx.r,
    total: pt + ta, end: addMo(new Date(d.getFullYear(), d.getMonth() + 1, 1), ongoing),
    perMo: (pt + ta) / dur,
    fLabel: "Pro-rated (" + rem + "/" + dm + " days)",
    fNote: fmtD(d) + " → end of " + d.toLocaleString("en-GB", { month: "short" })
  };
}

function StarsC({ rating }) {
  return (
    <span style={{ display: "inline-flex", gap: 1 }}>
      {[0,1,2,3,4].map(function(i) {
        return <Star key={i} size={9} fill={i < Math.floor(rating) ? "#FFA500" : "#ddd"} color="transparent" strokeWidth={0} />;
      })}
    </span>
  );
}

function ProgBar({ step, maxS, go, mob }) {
  var steps = [{ id: 1, l: "Location" },{ id: 2, l: "Storage" },{ id: 3, l: "Browse" },{ id: 4, l: "Book" }];
  return (
    <div style={{ position: "sticky", top: 0, zIndex: 60, background: "rgba(255,255,255,.97)", backdropFilter: "blur(12px)", borderBottom: "1px solid " + BD, padding: mob ? "7px 14px" : "9px 32px" }}>
      <div style={{ maxWidth: 900, margin: "0 auto", display: "flex", alignItems: "center" }}>
        {steps.map(function(s, i) {
          var done = step > s.id;
          var act = step === s.id;
          var reach = s.id <= maxS;
          return (
            <React.Fragment key={s.id}>
              <button onClick={function() { if (reach && s.id < step) go(s.id); }} style={{ display: "flex", alignItems: "center", gap: mob ? 3 : 5, background: "none", border: "none", cursor: reach && s.id < step ? "pointer" : "default", fontFamily: FT, padding: 0, opacity: reach ? 1 : 0.35 }}>
                <div style={{ width: mob ? 18 : 22, height: mob ? 18 : 22, borderRadius: "50%", background: done ? GR : act ? N : BD, color: done || act ? "#fff" : MT, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 9, fontWeight: 800 }}>
                  {done ? <Check size={10} /> : s.id}
                </div>
                <span style={{ fontSize: mob ? 9 : 11, fontWeight: act ? 800 : 600, color: act ? N : done ? GR : MT }}>{s.l}</span>
              </button>
              {i < 3 && <div style={{ flex: 1, height: 2, margin: "0 6px", background: done ? GR : BD, minWidth: mob ? 10 : 24 }} />}
            </React.Fragment>
          );
        })}
      </div>
    </div>
  );
}

function Step1({ mob, facs, setFacs, onNext }) {
  var isU = facs.includes("undecided");
  function tog(k) {
    if (k === "undecided") { setFacs(["undecided"]); return; }
    var next = facs.filter(function(x) { return x !== "undecided"; });
    if (next.includes(k)) next = next.filter(function(x) { return x !== k; });
    else next = next.concat([k]);
    setFacs(next);
  }
  return (
    <div style={{ maxWidth: 900, margin: "0 auto", padding: mob ? "14px 16px" : "20px 24px" }}>
      <h2 style={{ fontSize: mob ? 18 : 22, fontWeight: 900, marginBottom: 12 }}>Where do you need storage?</h2>
      <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
        {Object.entries(FAC).map(function([k, f]) {
          var ac = facs.includes(k);
          return (
            <button key={k} onClick={function() { tog(k); }} style={{ display: "flex", alignItems: "center", gap: 10, padding: "10px 12px", borderRadius: 10, border: "1.5px solid " + (ac ? N : BD), background: ac ? N + "08" : "#fff", cursor: "pointer", fontFamily: FT, textAlign: "left", width: "100%" }}>
              <div style={{ width: 20, height: 20, borderRadius: "50%", border: "2px solid " + (ac ? N : BD), background: ac ? N : "#fff", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>{ac && <Check size={11} color="#fff" />}</div>
              <div style={{ flex: 1 }}>
                <div style={{ fontSize: 13, fontWeight: 700 }}>{f.name}</div>
                <div style={{ fontSize: 9, color: MT }}>{f.addr}</div>
                <div style={{ display: "flex", alignItems: "center", gap: 3, marginTop: 2 }}>
                  <StarsC rating={f.rating} />
                  <span style={{ fontSize: 8, fontWeight: 700, color: "#FFA500" }}>{f.rating}</span>
                  <span style={{ fontSize: 8, color: MT }}>({f.reviews}) • {f.units} units</span>
                </div>
              </div>
            </button>
          );
        })}
        <button onClick={function() { tog("undecided"); }} style={{ display: "flex", alignItems: "center", gap: 10, padding: "10px 12px", borderRadius: 10, border: "1.5px dashed " + (isU ? AM : BD), background: isU ? AM + "08" : "#fff", cursor: "pointer", fontFamily: FT, width: "100%" }}>
          <div style={{ width: 20, height: 20, borderRadius: "50%", border: "2px solid " + (isU ? AM : BD), background: isU ? AM : "#fff", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>{isU && <Check size={11} color="#fff" />}</div>
          <div style={{ textAlign: "left" }}><div style={{ fontSize: 13, fontWeight: 700, color: isU ? AM : MT }}>Not decided yet</div></div>
        </button>
      </div>
      <button onClick={onNext} disabled={facs.length === 0} style={{ width: "100%", marginTop: 16, padding: "12px 0", borderRadius: 10, border: "none", background: facs.length ? N : BD, color: facs.length ? "#fff" : MT, fontSize: 13, fontWeight: 800, fontFamily: FT, cursor: facs.length ? "pointer" : "not-allowed", display: "flex", alignItems: "center", justifyContent: "center", gap: 7 }}>
        Next <ArrowRight size={14} />
      </button>
    </div>
  );
}

function Step2({ mob, ss, setSS, onNext }) {
  var tab = ss.tab, purs = ss.purs, clim = ss.clim, climAuto = ss.climAuto, manSz = ss.manSz, photos = ss.photos;
  function set(p) { setSS(function(prev) { return Object.assign({}, prev, p); }); }
  function togP(id) { set({ purs: purs.includes(id) ? purs.filter(function(x) { return x !== id; }) : purs.concat([id]) }); }

  var sugClim = useMemo(function() {
    if (!purs.length) return null;
    var cs = purs.map(function(id) { var p = PUR.find(function(x) { return x.id === id; }); return p ? p.cl : null; }).filter(Boolean);
    var pri = ["Refrig","AC+DC","AC","DC","None"];
    for (var i = 0; i < pri.length; i++) { if (cs.includes(pri[i])) return pri[i]; }
    return null;
  }, [purs]);

  useEffect(function() { if (climAuto && sugClim) set({ clim: sugClim }); }, [sugClim, climAuto]);

  var recPurs = useMemo(function() { if (!clim) return []; var c = CLM.find(function(x) { return x.id === clim; }); return c ? c.purs : []; }, [clim]);
  var totalSq = useMemo(function() { return purs.reduce(function(s, id) { var p = PUR.find(function(x) { return x.id === id; }); return s + (p ? p.sq : 0); }, 0); }, [purs]);

  function addPhotos(files) {
    var toAdd = Array.from(files).slice(0, 10 - photos.length);
    Promise.all(toAdd.map(function(f) { return new Promise(function(res) { var r = new FileReader(); r.onload = function(e) { res(e.target.result); }; r.readAsDataURL(f); }); })).then(function(urls) {
      set({ photos: photos.concat(urls.map(function(url) { return { id: Date.now() + Math.random(), url: url, est: null, busy: false, editing: false, prompt: "" }; })) });
    });
  }
  function upP(id, patch) { set({ photos: photos.map(function(p) { return p.id === id ? Object.assign({}, p, patch) : p; }) }); }
  function rmP(id) { set({ photos: photos.filter(function(p) { return p.id !== id; }) }); }
  function analyzeP(id) {
    upP(id, { busy: true, est: null });
    setTimeout(function() { var e = Object.assign({}, AI_POOL[Math.floor(Math.random() * AI_POOL.length)]); e.conf = 75 + Math.floor(Math.random() * 20); upP(id, { busy: false, est: e }); }, 1200 + Math.random() * 800);
  }
  function reanalyze(id) {
    var ph = photos.find(function(p) { return p.id === id; }); if (!ph) return;
    var prompt = ph.prompt;
    upP(id, { busy: true, est: null, editing: false });
    setTimeout(function() { var e = Object.assign({}, AI_POOL[Math.floor(Math.random() * AI_POOL.length)]); e.conf = 88; e.items = prompt || e.items; upP(id, { busy: false, est: e, prompt: "" }); }, 1000);
  }

  var pTotal = useMemo(function() { return photos.reduce(function(s, p) { return s + (p.est ? p.est.sq : 0); }, 0); }, [photos]);
  var pCount = photos.filter(function(p) { return p.est; }).length;

  var canNext = tab === "type" ? purs.length > 0 : tab === "photo" ? pCount > 0 : !!manSz;

  function doNext() {
    var out = { method: tab, purposeIds: purs, climate: clim, sqftEst: null, size: "All" };
    if (tab === "type") { out.sqftEst = totalSq; out.size = toSz(totalSq); }
    else if (tab === "photo") { out.sqftEst = pTotal; out.size = toSz(pTotal); }
    else { out.size = manSz; }
    onNext(out);
  }

  var tabs = [{ id: "type", l: "Purpose", I: Package },{ id: "photo", l: "AI Photo", I: Camera },{ id: "manual", l: "I Know", I: SlidersHorizontal }];

  return (
    <div style={{ maxWidth: 800, margin: "0 auto", padding: mob ? "14px 16px" : "20px 24px" }}>
      <h2 style={{ fontSize: mob ? 18 : 22, fontWeight: 900, marginBottom: 4 }}>What do you need to store?</h2>
      <p style={{ fontSize: 11, color: MT, marginBottom: 12 }}>Climate adapts to your purpose selection.</p>

      <div style={{ background: "#fff", borderRadius: RAD, padding: 12, boxShadow: SH, marginBottom: 12 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 5, marginBottom: 7 }}>
          <Thermometer size={12} color={N} /><span style={{ fontSize: 12, fontWeight: 800 }}>Climate</span>
          {climAuto && sugClim && <span style={{ fontSize: 8, fontWeight: 700, background: GR + "12", color: GR, padding: "2px 5px", borderRadius: 4 }}>Auto</span>}
          {!climAuto && sugClim && <button onClick={function() { set({ climAuto: true, clim: sugClim }); }} style={{ fontSize: 9, fontWeight: 700, color: BL, background: "none", border: "none", cursor: "pointer", fontFamily: FT, marginLeft: "auto" }}>Reset</button>}
        </div>
        <div className="cs">
          <button onClick={function() { set({ climAuto: false, clim: null }); }} style={{ padding: "5px 10px", borderRadius: 7, border: "1.5px solid " + (!clim ? N : BD), background: !clim ? N + "08" : "#fff", fontSize: 10, fontWeight: 700, fontFamily: FT, cursor: "pointer", color: !clim ? N : MT, flexShrink: 0 }}>Any</button>
          {CLM.map(function(c) { var I = c.icon; var a = clim === c.id; return (
            <button key={c.id} onClick={function() { set({ climAuto: false, clim: a ? null : c.id }); }} style={{ display: "flex", alignItems: "center", gap: 3, padding: "5px 9px", borderRadius: 7, border: "1.5px solid " + (a ? N : BD), background: a ? N + "08" : "#fff", fontSize: 10, fontWeight: 600, fontFamily: FT, cursor: "pointer", color: a ? N : MT, flexShrink: 0 }}><I size={10} />{c.short}</button>
          ); })}
        </div>
      </div>

      <div style={{ display: "flex", background: "#fff", borderRadius: 9, padding: 3, boxShadow: SH, marginBottom: 12 }}>
        {tabs.map(function(t) { var a = tab === t.id; var I = t.I; return (
          <button key={t.id} onClick={function() { set({ tab: t.id }); }} style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", gap: 4, padding: "9px 4px", borderRadius: 7, border: "none", background: a ? N : "transparent", color: a ? "#fff" : MT, fontSize: mob ? 10 : 12, fontWeight: 700, fontFamily: FT, cursor: "pointer" }}><I size={13} />{t.l}</button>
        ); })}
      </div>

      <div style={{ background: "#fff", borderRadius: RAD, padding: mob ? 12 : 16, boxShadow: SH, marginBottom: 14 }}>
        {tab === "type" && (
          <div>
            <div style={{ display: "grid", gridTemplateColumns: mob ? "repeat(2,1fr)" : "repeat(4,1fr)", gap: 7 }}>
              {PUR.map(function(p) { var I = p.icon; var a = purs.includes(p.id); var rec = recPurs.includes(p.id) && !a; return (
                <button key={p.id} onClick={function() { togP(p.id); }} style={{ padding: "10px 6px", borderRadius: 10, border: "2px solid " + (a ? N : rec ? BL + "60" : BD), background: a ? N + "06" : rec ? BL + "04" : "#fff", cursor: "pointer", fontFamily: FT, textAlign: "center", position: "relative" }}>
                  {a && <div style={{ position: "absolute", top: 4, right: 4, width: 14, height: 14, borderRadius: "50%", background: N, display: "flex", alignItems: "center", justifyContent: "center" }}><Check size={8} color="#fff" /></div>}
                  {rec && <div style={{ position: "absolute", top: 4, right: 4, fontSize: 7, fontWeight: 700, color: BL, background: BL + "15", padding: "1px 4px", borderRadius: 3 }}>rec</div>}
                  <I size={18} color={a ? N : MT} style={{ marginBottom: 2 }} /><div style={{ fontSize: 10, fontWeight: 700 }}>{p.label}</div><div style={{ fontSize: 8, color: MT }}>~{p.sq}sqft</div>
                </button>
              ); })}
            </div>
            {purs.length > 0 && <div style={{ marginTop: 10, padding: "8px 10px", borderRadius: 8, background: N + "04", border: "1px solid " + BD, display: "flex", alignItems: "center", gap: 6, fontSize: 10, fontWeight: 700, color: N }}><Sparkles size={12} />{purs.length} selected → ~{totalSq}sqft ({toSz(totalSq)})</div>}
          </div>
        )}

        {tab === "photo" && (
          <div>
            <p style={{ fontSize: 10, color: MT, marginBottom: 10 }}>Upload up to 10 photos. Each = 1 item. Tap pencil to correct.</p>
            <div style={{ display: "grid", gridTemplateColumns: mob ? "repeat(3,1fr)" : "repeat(5,1fr)", gap: 7, marginBottom: 10 }}>
              {photos.map(function(p) { return (
                <div key={p.id} style={{ borderRadius: 8, overflow: "hidden", border: "1.5px solid " + (p.est ? GR : BD), background: "#FAFBFC" }}>
                  <div style={{ aspectRatio: "1", position: "relative" }}>
                    <img src={p.url} alt="" style={{ width: "100%", height: "100%", objectFit: "cover", display: "block" }} />
                    <button onClick={function() { rmP(p.id); }} style={{ position: "absolute", top: 2, right: 2, width: 16, height: 16, borderRadius: "50%", background: "rgba(0,0,0,.5)", color: "#fff", border: "none", cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center" }}><X size={8} /></button>
                    {p.busy && <div style={{ position: "absolute", inset: 0, background: "rgba(27,42,74,.7)", display: "flex", alignItems: "center", justifyContent: "center" }}><Loader2 size={14} color="#fff" style={{ animation: "spin 1s linear infinite" }} /></div>}
                  </div>
                  {p.est ? (
                    <div style={{ padding: "4px 5px" }}>
                      <div style={{ fontSize: 9, fontWeight: 700, color: N }}>{p.est.items}</div>
                      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginTop: 2 }}>
                        <span style={{ fontSize: 11, fontWeight: 800 }}>{p.est.sq}sqft</span>
                        <button onClick={function() { upP(p.id, { editing: !p.editing }); }} style={{ background: "none", border: "none", cursor: "pointer", color: MT, padding: 0 }}><Pencil size={9} /></button>
                      </div>
                      {p.editing && (
                        <div style={{ marginTop: 3, display: "flex", gap: 2 }}>
                          <input type="text" placeholder="Correct..." value={p.prompt} onChange={function(e) { upP(p.id, { prompt: e.target.value }); }} style={{ flex: 1, padding: "3px 5px", borderRadius: 4, border: "1px solid " + BD, fontSize: 9, fontFamily: FT, outline: "none" }} />
                          <button onClick={function() { reanalyze(p.id); }} style={{ width: 20, height: 20, borderRadius: 4, background: N, color: "#fff", border: "none", cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center" }}><Send size={8} /></button>
                        </div>
                      )}
                    </div>
                  ) : (
                    !p.busy && <button onClick={function() { analyzeP(p.id); }} style={{ width: "100%", padding: "4px 0", border: "none", background: N, color: "#fff", fontSize: 9, fontWeight: 700, fontFamily: FT, cursor: "pointer" }}>Analyze</button>
                  )}
                </div>
              ); })}
              {photos.length < 10 && (
                <label style={{ aspectRatio: "1", borderRadius: 8, border: "2px dashed " + BD, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", cursor: "pointer", gap: 3, background: "#FAFBFC" }}>
                  <Plus size={16} color={MT} /><span style={{ fontSize: 8, color: MT }}>{photos.length}/10</span>
                  <input type="file" accept="image/*" multiple onChange={function(e) { addPhotos(e.target.files); }} style={{ display: "none" }} />
                </label>
              )}
            </div>
            {pCount > 0 && <div style={{ padding: "8px 10px", borderRadius: 8, background: GR + "08", border: "1.5px solid " + GR + "20", display: "flex", alignItems: "center", justifyContent: "space-between" }}><span style={{ fontSize: 11, fontWeight: 800, color: GR }}>Total ({pCount} items)</span><span style={{ fontSize: 14, fontWeight: 900, color: N }}>{pTotal}sqft ({toSz(pTotal)})</span></div>}
          </div>
        )}

        {tab === "manual" && (
          <div style={{ display: "grid", gridTemplateColumns: mob ? "repeat(2,1fr)" : "repeat(4,1fr)", gap: 7 }}>
            {SZS.filter(function(s) { return s.l !== "All"; }).map(function(s) { var a = manSz === s.l; return (
              <button key={s.l} onClick={function() { set({ manSz: a ? null : s.l }); }} style={{ padding: "14px 6px", borderRadius: 10, border: "2px solid " + (a ? N : BD), background: a ? N + "06" : "#fff", cursor: "pointer", fontFamily: FT, textAlign: "center" }}>
                <div style={{ fontSize: 22, fontWeight: 900, color: a ? N : MT }}>{s.l}</div>
                <div style={{ fontSize: 10, color: MT, fontWeight: 600 }}>{s.sub}sqft</div>
              </button>
            ); })}
          </div>
        )}
      </div>

      <button onClick={doNext} disabled={!canNext} style={{ width: "100%", padding: "12px 0", borderRadius: 10, border: "none", background: canNext ? R : BD, color: canNext ? "#fff" : MT, fontSize: 13, fontWeight: 800, fontFamily: FT, cursor: canNext ? "pointer" : "not-allowed", display: "flex", alignItems: "center", justifyContent: "center", gap: 7 }}>Browse Units <ArrowRight size={14} /></button>
    </div>
  );
}

function Step3({ mob, profile, facs, onSelect }) {
  var allF = facs.includes("undecided");
  var multi = facs.filter(function(k) { return k !== "undecided"; }).length > 1;
  var fkeys = facs.filter(function(k) { return k !== "undecided"; });
  var _s = useState(profile.size || "All"); var sf = _s[0]; var setSf = _s[1];
  var _r = useState([0,200]); var rng = _r[0]; var setRng = _r[1];
  var _c = useState(profile.climate ? [profile.climate] : []); var cf = _c[0]; var setCf = _c[1];
  var _d = useState(null); var disc = _d[0]; var setDisc = _d[1];
  var _cc = useState(""); var cc = _cc[0]; var setCc = _cc[1];
  var _cr = useState(null); var cr = _cr[0]; var setCr = _cr[1];
  var _m = useState(profile.intent === "price" ? "best_price" : "selection"); var mode = _m[0]; var setMode = _m[1];
  var _sp = useState(false); var showP = _sp[0]; var setShowP = _sp[1];

  var calcP = useCallback(function(b) { var p = b; if (disc && disc.type === "percent") p *= (1 - disc.pct); if (cr && cr.valid) p *= (1 - cr.pct); return p; }, [disc, cr]);
  var hasDsc = disc || (cr && cr.valid);
  var isFF = disc && disc.type === "first_free";
  var modes = [{ id: "selection", l: "Yours", I: SlidersHorizontal },{ id: "best_price", l: "Best $", I: DollarSign }];
  if (!allF && !multi) modes = modes.concat([{ id: "nearby", l: "Nearby", I: MapPinned },{ id: "nearby_best", l: "Near $", I: Zap }]);

  var units = useMemo(function() {
    var l = UNITS.slice();
    var isN = mode === "nearby" || mode === "nearby_best";
    var isB = mode === "best_price" || mode === "nearby_best";
    if (!allF) {
      if (isN && fkeys.length === 1) l = l.filter(function(u) { return u.fac !== fkeys[0]; });
      else l = l.filter(function(u) { return fkeys.includes(u.fac); });
    }
    if (!isB) { l = l.filter(function(u) { return u.sqft >= rng[0] && u.sqft <= rng[1]; }); if (cf.length) l = l.filter(function(u) { return cf.includes(u.clim); }); }
    if (isB || allF) l.sort(function(a, b) { return calcP(a.price) - calcP(b.price); });
    return l;
  }, [sf, rng, cf, mode, calcP, fkeys, allF]);

  return (
    <div>
      <nav style={{ background: "rgba(255,255,255,.96)", backdropFilter: "blur(12px)", borderBottom: "1px solid " + BD }}>
        <div style={{ display: "flex", alignItems: "center", gap: 6, padding: "7px 14px" }}>
          <div className="cs" style={{ flex: 1 }}>{SZS.map(function(s) { return <button key={s.l} onClick={function() { setSf(s.l); var b = SZS.find(function(x) { return x.l === s.l; }); if (b && s.l !== "All") setRng(b.r); else setRng([0,200]); }} style={{ padding: "4px 10px", borderRadius: 999, border: "none", fontSize: 10, fontWeight: 700, fontFamily: FT, cursor: "pointer", flexShrink: 0, background: sf === s.l ? N : "#EDEEF2", color: sf === s.l ? "#fff" : MT }}>{s.l}</button>; })}</div>
        </div>
      </nav>
      <main style={{ maxWidth: 1280, margin: "0 auto", padding: mob ? "12px 14px 100px" : "18px 24px 50px", display: "flex", flexDirection: mob ? "column" : "row", gap: mob ? 12 : 20 }}>
        <aside style={{ width: mob ? "100%" : "28%", minWidth: mob ? "auto" : 230, flexShrink: 0, display: "flex", flexDirection: "column", gap: 12 }}>
          <div style={{ background: "#fff", borderRadius: RAD, padding: 12, boxShadow: SH }}><div style={{ display: "flex", alignItems: "center", gap: 5 }}><MapPin size={12} color={R} /><span style={{ fontSize: 12, fontWeight: 800 }}>{allF ? "All Locations" : multi ? fkeys.length + " sites" : FAC[fkeys[0]] && FAC[fkeys[0]].name}</span></div></div>
          <div style={{ background: "#fff", borderRadius: RAD, padding: 12, boxShadow: SH, borderTop: "3px solid " + R }}>
            <button onClick={function() { if (mob) setShowP(!showP); }} style={{ display: "flex", alignItems: "center", justifyContent: "space-between", width: "100%", background: "none", border: "none", cursor: mob ? "pointer" : "default", fontFamily: FT, padding: 0 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 5 }}><Tag size={11} color={R} /><span style={{ fontSize: 12, fontWeight: 800 }}>Promotions</span></div>
              {mob && (showP ? <ChevronUp size={13} color={MT} /> : <ChevronDown size={13} color={MT} />)}
            </button>
            {(!mob || showP) && (
              <div style={{ marginTop: 8 }}>
                <div style={{ display: "flex", flexDirection: "column", gap: 5 }}>
                  {DSC.map(function(d) { var a = disc && disc.id === d.id; return (
                    <button key={d.id} onClick={function() { setDisc(a ? null : d); }} style={{ textAlign: "left", padding: "8px 10px", borderRadius: 7, border: "1.5px solid " + (a ? N : BD), background: a ? N + "06" : "#fff", cursor: "pointer", fontFamily: FT, position: "relative" }}>
                      {d.badge && <span style={{ position: "absolute", top: -5, right: 6, background: R, color: "#fff", fontSize: 7, fontWeight: 800, padding: "1px 5px", borderRadius: 3 }}>{d.badge}</span>}
                      <div style={{ fontSize: 11, fontWeight: 700 }}>{d.name}</div>
                    </button>
                  ); })}
                </div>
                <div style={{ marginTop: 8, paddingTop: 8, borderTop: "1px solid " + BD, display: "flex", gap: 5 }}>
                  <input type="text" placeholder="Promo code" value={cc} onChange={function(e) { setCc(e.target.value.toUpperCase()); setCr(null); }} style={{ flex: 1, padding: "6px 8px", borderRadius: 6, border: "1.5px solid " + BD, fontSize: 10, fontWeight: 600, fontFamily: FT, outline: "none", background: "#FAFBFC", color: N }} />
                  <button onClick={function() { var c = cc.trim().toUpperCase(); setCr(COUPS[c] !== undefined ? { valid: true, pct: COUPS[c], code: c } : { valid: false }); }} style={{ padding: "0 10px", borderRadius: 6, border: "none", background: N, color: "#fff", fontSize: 10, fontWeight: 700, fontFamily: FT, cursor: "pointer" }}>Apply</button>
                </div>
                {cr && cr.valid && <div style={{ marginTop: 4, fontSize: 9, color: GR, fontWeight: 700 }}>{cr.code} applied!</div>}
                {cr && !cr.valid && <div style={{ marginTop: 4, fontSize: 9, color: R, fontWeight: 700 }}>Invalid code</div>}
              </div>
            )}
          </div>
        </aside>
        <section style={{ flex: 1, minWidth: 0 }}>
          <div style={{ display: "flex", background: N, borderRadius: 8, padding: 3, marginBottom: 10 }}>
            {modes.map(function(m) { var a = mode === m.id; var I = m.I; return (
              <button key={m.id} onClick={function() { setMode(m.id); }} style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", gap: 3, padding: "8px 2px", borderRadius: 6, border: "none", background: a ? "#fff" : "transparent", color: a ? N : "rgba(255,255,255,.45)", fontSize: 10, fontWeight: 700, fontFamily: FT, cursor: "pointer" }}><I size={12} />{m.l}</button>
            ); })}
          </div>
          <div style={{ marginBottom: 10 }}><h2 style={{ fontSize: 16, fontWeight: 800 }}>Available Units</h2><p style={{ fontSize: 10, color: MT }}>{units.length} units — tap to book</p></div>
          <div style={{ display: "grid", gridTemplateColumns: mob ? "repeat(2,1fr)" : "repeat(3,1fr)", gap: mob ? 8 : 10 }}>
            {units.map(function(u) {
              var dp = calcP(u.price);
              var ss = hasDsc && dp < u.price;
              var uf = FAC[u.fac];
              var fb = allF || multi;
              return (
                <div key={u.id} className="uc" onClick={function() { onSelect(u, disc, cr); }} style={{ background: "#fff", borderRadius: RAD, padding: mob ? 10 : 12, boxShadow: SH, cursor: "pointer", display: "flex", flexDirection: "column" }}>
                  {fb && <div style={{ fontSize: 8, fontWeight: 700, color: R, background: R + "10", padding: "1px 5px", borderRadius: 3, marginBottom: 4, display: "inline-flex", alignItems: "center", gap: 2, alignSelf: "flex-start" }}><MapPin size={7} />{uf && uf.name}</div>}
                  <div style={{ display: "flex", alignItems: "center", gap: 5, marginBottom: 5 }}><span style={{ width: 20, height: 20, borderRadius: "50%", background: N, color: "#fff", fontSize: 9, fontWeight: 800, display: "flex", alignItems: "center", justifyContent: "center" }}>{u.sz}</span><span style={{ fontSize: 10, fontWeight: 600, color: MT }}>Locker {u.id}</span></div>
                  <div style={{ background: "#F8F9FB", borderRadius: 6, padding: "6px 8px", marginBottom: 5 }}><div style={{ fontSize: 14, fontWeight: 800 }}>{u.dims}ft</div><div style={{ fontSize: 8, color: MT, fontWeight: 600 }}>{u.sqft}sqft</div></div>
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 2, marginBottom: 6, flex: 1 }}>{u.tags.map(function(t) { return <span key={t} style={{ padding: "1px 4px", background: "#F0F1F4", color: MT, borderRadius: 3, fontSize: 8, fontWeight: 600 }}>{t}</span>; })}<span style={{ padding: "1px 4px", background: "#EBF4FF", color: BL, borderRadius: 3, fontSize: 8, fontWeight: 600 }}>{u.clim}</span></div>
                  <div style={{ borderTop: "1px solid " + BD, paddingTop: 6, display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                    <div>
                      {isFF && <div style={{ fontSize: 9, fontWeight: 800, color: GR }}>1st Mo FREE</div>}
                      {ss && !isFF && <div style={{ fontSize: 9, color: MT, textDecoration: "line-through" }}>SGD {u.price.toFixed(2)}</div>}
                      <div style={{ display: "flex", alignItems: "baseline", gap: 2 }}><span style={{ fontSize: 14, fontWeight: 900, color: (ss || isFF) ? R : N }}>SGD {isFF ? u.price.toFixed(2) : dp.toFixed(2)}</span><span style={{ fontSize: 9, color: MT }}>/mo</span></div>
                    </div>
                    <div style={{ width: 28, height: 28, borderRadius: 6, background: R, color: "#fff", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}><ChevronRight size={13} /></div>
                  </div>
                </div>
              );
            })}
          </div>
        </section>
      </main>
    </div>
  );
}

function Step4({ mob, unit, initDisc, initCoupon }) {
  var f = FAC[unit.fac];
  var _mi = useState(function() { var t = new Date(); t.setDate(t.getDate() + 3); return t.toISOString().split("T")[0]; }); var moveIn = _mi[0]; var setMI = _mi[1];
  var _d = useState(initDisc); var disc = _d[0]; var setDisc = _d[1];
  var _du = useState(function() { return initDisc ? initDisc.durs[0] : 3; }); var dur = _du[0]; var setDur = _du[1];
  var _in = useState("standard"); var ins = _in[0]; var setIns = _in[1];
  var _ac = useState(null); var act = _ac[0]; var setAct = _ac[1];
  var _ans = useState({}); var answers = _ans[0]; var setAnswers = _ans[1];

  function setAnswer(qId, value) {
    setAnswers(function(prev) { var n = Object.assign({}, prev); n[qId] = value; return n; });
  }
  function toggleMulti(qId, optId) {
    setAnswers(function(prev) {
      var n = Object.assign({}, prev);
      var cur = n[qId] || [];
      if (cur.includes(optId)) n[qId] = cur.filter(function(x) { return x !== optId; });
      else n[qId] = cur.concat([optId]);
      return n;
    });
  }

  var activeQuestions = useMemo(function() {
    var ctx = { disc: disc, unit: unit, dur: dur, facility: f };
    return CHECKOUT_QUESTIONS.filter(function(q) {
      return q.active && q.trigger(ctx);
    });
  }, [disc, unit, dur, f]);

  var requiredUnanswered = useMemo(function() {
    return activeQuestions.filter(function(q) {
      if (!q.required) return false;
      var a = answers[q.id];
      if (!a) return true;
      if (q.type === "multi") return a.length === 0;
      if (q.type === "text") return a.trim() === "";
      return false;
    });
  }, [activeQuestions, answers]);

  var avD = useMemo(function() { return disc ? DURS.filter(function(d) { return disc.durs.includes(d.m); }) : DURS; }, [disc]);
  useEffect(function() { if (disc && !disc.durs.includes(dur)) setDur(disc.durs[0]); }, [disc, dur]);

  var bill = useMemo(function() { return calcBill(moveIn, dur, unit.price, ins, f.region, disc, initCoupon); }, [moveIn, dur, unit.price, ins, f.region, disc, initCoupon]);
  if (!bill) return <div>Select a move-in date</div>;

  return (
    <div style={{ maxWidth: 1100, margin: "0 auto", padding: mob ? "12px 14px 120px" : "20px 24px 60px" }}>
      <div style={{ background: "#fff", borderRadius: RAD, padding: mob ? 14 : 18, boxShadow: SH, marginBottom: 14, display: "flex", alignItems: "center", gap: 12 }}>
        <div style={{ width: mob ? 46 : 56, height: mob ? 46 : 56, borderRadius: 10, background: N, color: "#fff", display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", flexShrink: 0 }}><div style={{ fontSize: mob ? 14 : 18, fontWeight: 900 }}>{unit.sz}</div><div style={{ fontSize: 8, opacity: 0.6 }}>{unit.sqft}sqft</div></div>
        <div style={{ flex: 1 }}><div style={{ fontSize: 10, color: MT, fontWeight: 700 }}>LOCKER {unit.id} • {f.name}</div><div style={{ fontSize: mob ? 15 : 17, fontWeight: 800 }}>{unit.dims}ft • {unit.clim}</div></div>
        <div style={{ textAlign: "right", flexShrink: 0 }}><div style={{ fontSize: mob ? 16 : 19, fontWeight: 900 }}>SGD {unit.price}</div><div style={{ fontSize: 9, color: MT }}>/mo</div></div>
      </div>

      <div style={{ display: "flex", flexDirection: mob ? "column" : "row", gap: 14 }}>
        <div style={{ flex: mob ? 1 : 1.1, display: "flex", flexDirection: "column", gap: 14, minWidth: 0 }}>
          <div style={{ background: "#fff", borderRadius: RAD, padding: mob ? 14 : 18, boxShadow: SH }}>
            <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 8 }}><MapPin size={14} color={R} /><div><div style={{ fontSize: 14, fontWeight: 800 }}>{f.name}</div><div style={{ display: "flex", alignItems: "center", gap: 3, marginTop: 1 }}><StarsC rating={f.rating} /><span style={{ fontSize: 9, fontWeight: 700, color: "#FFA500" }}>{f.rating}</span><span style={{ fontSize: 9, color: MT }}>({f.reviews})</span></div></div></div>
            <div style={{ fontSize: 11, color: MT, marginBottom: 8 }}>{f.addr}, {f.postal}</div>
            <div style={{ display: "flex", flexDirection: "column", gap: 4, fontSize: 10 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 5 }}><Train size={11} color={BL} />{f.mrt}</div>
              <div style={{ display: "flex", alignItems: "center", gap: 5 }}><Bus size={11} color={BL} />Bus {f.bus}</div>
            </div>
          </div>
          <div style={{ background: "#fff", borderRadius: RAD, padding: mob ? 14 : 18, boxShadow: SH }}>
            <div style={{ fontSize: 12, fontWeight: 800, marginBottom: 8 }}>Move-in Date</div>
            <input type="date" value={moveIn} min={new Date().toISOString().split("T")[0]} onChange={function(e) { setMI(e.target.value); }} style={{ width: "100%", boxSizing: "border-box", padding: "10px 12px", borderRadius: 9, border: "1.5px solid " + BD, fontSize: 13, fontWeight: 600, fontFamily: FT, outline: "none", background: "#FAFBFC", color: N }} />
          </div>
          <div style={{ background: "#fff", borderRadius: RAD, padding: mob ? 14 : 18, boxShadow: SH }}>
            <div style={{ fontSize: 12, fontWeight: 800, marginBottom: 8 }}>Discount</div>
            <div style={{ display: "flex", flexDirection: "column", gap: 5 }}>{DSC.map(function(d) { var a = disc && disc.id === d.id; return (
              <button key={d.id} onClick={function() { setDisc(a ? null : d); }} style={{ textAlign: "left", padding: "8px 10px", borderRadius: 7, border: "1.5px solid " + (a ? N : BD), background: a ? N + "06" : "#fff", cursor: "pointer", fontFamily: FT }}><div style={{ fontSize: 11, fontWeight: 700 }}>{d.name}</div><div style={{ fontSize: 8, color: AM }}>{d.durs.length === 1 ? "Locks to " + d.durs[0] + "mo" : "Min " + d.durs[0] + "mo"}</div></button>
            ); })}</div>
          </div>
          <div style={{ background: "#fff", borderRadius: RAD, padding: mob ? 14 : 18, boxShadow: SH }}>
            <div style={{ fontSize: 12, fontWeight: 800, marginBottom: 8 }}>Duration</div>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 6 }}>{DURS.map(function(d) { var ok = avD.find(function(x) { return x.m === d.m; }); var ac = dur === d.m; return (
              <button key={d.m} onClick={function() { if (ok) setDur(d.m); }} disabled={!ok} style={{ padding: "10px 4px", borderRadius: 9, border: "2px solid " + (ac ? N : BD), background: !ok ? "#F5F6F8" : ac ? N + "06" : "#fff", cursor: ok ? "pointer" : "not-allowed", opacity: ok ? 1 : 0.3, fontFamily: FT, textAlign: "center" }}><div style={{ fontSize: 13, fontWeight: 800, color: ac ? N : MT }}>{d.l}</div></button>
            ); })}</div>
          </div>
          <div style={{ background: "#fff", borderRadius: RAD, padding: mob ? 14 : 18, boxShadow: SH }}>
            <div style={{ fontSize: 12, fontWeight: 800, marginBottom: 8 }}>Insurance</div>
            <div style={{ display: "flex", flexDirection: "column", gap: 5 }}>{INS.map(function(p) { var a = ins === p.id; return (
              <button key={p.id} onClick={function() { setIns(p.id); }} style={{ padding: "8px 10px", borderRadius: 8, border: "1.5px solid " + (a ? N : BD), background: a ? N + "06" : "#fff", cursor: "pointer", fontFamily: FT, display: "flex", alignItems: "center", gap: 8 }}>
                <div style={{ width: 14, height: 14, borderRadius: "50%", border: "2px solid " + (a ? N : BD), background: a ? N : "#fff", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>{a && <div style={{ width: 4, height: 4, borderRadius: "50%", background: "#fff" }} />}</div>
                <div style={{ flex: 1, textAlign: "left" }}><div style={{ fontSize: 11, fontWeight: 700 }}>{p.name}</div></div>
                <div style={{ fontSize: 11, fontWeight: 800, color: p.p === 0 ? GR : N }}>{p.p === 0 ? "FREE" : "+$" + p.p + "/mo"}</div>
              </button>
            ); })}</div>
          </div>

          {/* CHECKOUT QUESTIONS MODULE — conditionally rendered */}
          {activeQuestions.length > 0 && (
            <div style={{ background: "#fff", borderRadius: RAD, padding: mob ? 14 : 18, boxShadow: SH, borderLeft: "4px solid " + BL }}>
              <div style={{ fontSize: 12, fontWeight: 800, marginBottom: 10, display: "flex", alignItems: "center", gap: 6 }}>
                <HelpCircle size={14} color={BL} />
                <span>A few quick questions</span>
                {requiredUnanswered.length > 0 && <span style={{ fontSize: 8, fontWeight: 700, background: R + "12", color: R, padding: "2px 6px", borderRadius: 4, marginLeft: "auto" }}>{requiredUnanswered.length} required</span>}
              </div>
              <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
                {activeQuestions.map(function(q) {
                  var QIcon = q.icon;
                  var val = answers[q.id];
                  var isAnswered = q.type === "multi" ? (val && val.length > 0) : q.type === "text" ? (val && val.trim() !== "") : !!val;
                  return (
                    <div key={q.id} style={{ padding: "10px 12px", borderRadius: 10, background: isAnswered ? GR + "04" : "#FAFBFC", border: "1.5px solid " + (isAnswered ? GR + "30" : BD), transition: "all 0.2s" }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 8 }}>
                        <QIcon size={13} color={isAnswered ? GR : N} />
                        <span style={{ fontSize: 11, fontWeight: 700, color: N, flex: 1 }}>{q.question}</span>
                        {q.required && <span style={{ fontSize: 7, fontWeight: 700, color: R, background: R + "12", padding: "1px 5px", borderRadius: 3 }}>Required</span>}
                        {q.badge && <span style={{ fontSize: 7, fontWeight: 700, color: BL, background: BL + "12", padding: "1px 5px", borderRadius: 3 }}>{q.badge}</span>}
                        {isAnswered && <CheckCircle2 size={13} color={GR} />}
                      </div>

                      {/* SINGLE SELECT */}
                      {q.type === "single" && (
                        <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                          {q.options.map(function(opt) {
                            var sel = val === opt.id;
                            return (
                              <button key={opt.id} onClick={function() { setAnswer(q.id, sel ? null : opt.id); }} style={{ display: "flex", alignItems: "center", gap: 8, padding: "7px 10px", borderRadius: 7, border: "1.5px solid " + (sel ? N : BD), background: sel ? N + "06" : "#fff", cursor: "pointer", fontFamily: FT, textAlign: "left" }}>
                                <div style={{ width: 14, height: 14, borderRadius: "50%", border: "2px solid " + (sel ? N : BD), background: sel ? N : "#fff", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
                                  {sel && <div style={{ width: 5, height: 5, borderRadius: "50%", background: "#fff" }} />}
                                </div>
                                <div style={{ flex: 1 }}>
                                  <div style={{ fontSize: 11, fontWeight: 700, color: sel ? N : MT }}>{opt.label}</div>
                                  {opt.desc && <div style={{ fontSize: 9, color: MT }}>{opt.desc}</div>}
                                </div>
                              </button>
                            );
                          })}
                        </div>
                      )}

                      {/* MULTI SELECT */}
                      {q.type === "multi" && (
                        <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                          {q.options.map(function(opt) {
                            var checked = val && val.includes(opt.id);
                            return (
                              <button key={opt.id} onClick={function() { toggleMulti(q.id, opt.id); }} style={{ display: "flex", alignItems: "center", gap: 8, padding: "7px 10px", borderRadius: 7, border: "1.5px solid " + (checked ? N : BD), background: checked ? N + "06" : "#fff", cursor: "pointer", fontFamily: FT, textAlign: "left" }}>
                                <div style={{ width: 14, height: 14, borderRadius: 4, border: "2px solid " + (checked ? N : BD), background: checked ? N : "#fff", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
                                  {checked && <Check size={9} color="#fff" />}
                                </div>
                                <div style={{ flex: 1 }}>
                                  <div style={{ fontSize: 11, fontWeight: 700, color: checked ? N : MT }}>{opt.label}</div>
                                  {opt.desc && <div style={{ fontSize: 9, color: MT }}>{opt.desc}</div>}
                                </div>
                              </button>
                            );
                          })}
                        </div>
                      )}

                      {/* TEXT INPUT */}
                      {q.type === "text" && (
                        <textarea
                          value={val || ""}
                          onChange={function(e) { setAnswer(q.id, e.target.value); }}
                          placeholder="Type your answer..."
                          rows={3}
                          style={{ width: "100%", boxSizing: "border-box", padding: "8px 10px", borderRadius: 7, border: "1.5px solid " + BD, fontSize: 11, fontWeight: 600, fontFamily: FT, outline: "none", background: "#fff", color: N, resize: "vertical" }}
                        />
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
          )}

        </div>

        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ background: "#fff", borderRadius: RAD, overflow: "hidden", boxShadow: SH, position: mob ? "static" : "sticky", top: 70 }}>
            <div style={{ background: "linear-gradient(135deg," + N + ",#2d3f63)", color: "#fff", padding: "14px 18px" }}>
              <div style={{ fontSize: 10, fontWeight: 700, letterSpacing: 1.5, opacity: 0.6, textTransform: "uppercase" }}>Total</div>
              <div style={{ fontSize: 28, fontWeight: 900, marginTop: 3 }}>SGD {bill.total.toFixed(2)}</div>
              <div style={{ fontSize: 10, opacity: 0.7, marginTop: 3 }}>≈ SGD {bill.perMo.toFixed(2)}/mo • {dur} mo</div>
            </div>
            <div style={{ padding: "14px 18px", display: "flex", flexDirection: "column", gap: 6, fontSize: 11 }}>
              <div style={{ display: "flex", justifyContent: "space-between" }}><div><div style={{ fontWeight: 700 }}>{bill.fLabel}</div><div style={{ fontSize: 9, color: MT }}>{bill.fNote}</div></div><div style={{ fontWeight: 700 }}>SGD {bill.first.toFixed(2)}</div></div>
              {bill.ongoing > 0 && <div style={{ display: "flex", justifyContent: "space-between" }}><span style={{ fontWeight: 700 }}>{bill.ongoing} × Full months</span><span style={{ fontWeight: 700 }}>SGD {bill.subseq.toFixed(2)}</span></div>}
              {bill.ff > 0 && <div style={{ display: "flex", justifyContent: "space-between", color: GR, fontWeight: 700 }}><span>{bill.discN}</span><span>-SGD {bill.ff.toFixed(2)}</span></div>}
              {bill.pd > 0 && <div style={{ display: "flex", justifyContent: "space-between", color: GR, fontWeight: 700 }}><span>{bill.discN}</span><span>-SGD {bill.pd.toFixed(2)}</span></div>}
              {bill.ca > 0 && <div style={{ display: "flex", justifyContent: "space-between", color: GR, fontWeight: 700 }}><span>Coupon {bill.cCode}</span><span>-SGD {bill.ca.toFixed(2)}</span></div>}
              {bill.ic > 0 && <div style={{ display: "flex", justifyContent: "space-between" }}><span style={{ fontWeight: 700 }}>Insurance</span><span style={{ fontWeight: 700 }}>SGD {bill.ic.toFixed(2)}</span></div>}
              <div style={{ borderTop: "1px solid " + BD, paddingTop: 6, display: "flex", justifyContent: "space-between", color: MT }}><span>Subtotal</span><span style={{ fontWeight: 700, color: N }}>SGD {bill.pt.toFixed(2)}</span></div>
              <div style={{ display: "flex", justifyContent: "space-between", color: MT }}><span>{bill.txL} ({(bill.txR * 100).toFixed(0)}%)</span><span style={{ fontWeight: 700, color: N }}>SGD {bill.ta.toFixed(2)}</span></div>
              <div style={{ borderTop: "2px solid " + N, paddingTop: 8, display: "flex", justifyContent: "space-between", alignItems: "baseline" }}><span style={{ fontSize: 13, fontWeight: 800 }}>Grand Total</span><span style={{ fontSize: 18, fontWeight: 900, color: R }}>SGD {bill.total.toFixed(2)}</span></div>
              <div style={{ fontSize: 9, color: MT, fontStyle: "italic" }}>Lease: {fmtD(moveIn)} → {fmtD(bill.end)}</div>
            </div>
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 7, marginTop: 14 }}>
            {requiredUnanswered.length > 0 && (
              <div style={{ padding: "8px 12px", borderRadius: 8, background: AM + "08", border: "1.5px solid " + AM + "25", fontSize: 10, fontWeight: 600, color: AM, display: "flex", alignItems: "center", gap: 6 }}>
                <AlertCircle size={13} />Please answer {requiredUnanswered.length} required question{requiredUnanswered.length > 1 ? "s" : ""} above to proceed.
              </div>
            )}
            <button onClick={function() { if (requiredUnanswered.length === 0) setAct("pay"); }} style={{ padding: "13px 0", borderRadius: 11, border: "none", background: requiredUnanswered.length > 0 ? BD : R, color: requiredUnanswered.length > 0 ? MT : "#fff", fontSize: 13, fontWeight: 800, fontFamily: FT, cursor: requiredUnanswered.length > 0 ? "not-allowed" : "pointer", display: "flex", alignItems: "center", justifyContent: "center", gap: 7, boxShadow: requiredUnanswered.length > 0 ? "none" : "0 4px 14px rgba(230,57,70,.3)", opacity: requiredUnanswered.length > 0 ? 0.6 : 1 }}><Lock size={15} />Pay Now — SGD {bill.total.toFixed(2)}</button>
            <button onClick={function() { if (requiredUnanswered.length === 0) setAct("res"); }} style={{ padding: "11px 0", borderRadius: 10, border: "2px solid " + N, background: "#fff", color: N, fontSize: 12, fontWeight: 800, fontFamily: FT, cursor: requiredUnanswered.length > 0 ? "not-allowed" : "pointer", display: "flex", alignItems: "center", justifyContent: "center", gap: 6, opacity: requiredUnanswered.length > 0 ? 0.6 : 1 }}><CreditCard size={13} />Reserve SGD 50</button>
            <div style={{ display: "flex", gap: 7 }}>
              <button onClick={function() { setAct("view"); }} style={{ flex: 1, padding: "10px", borderRadius: 9, border: "1.5px solid " + BD, background: "#fff", color: N, fontSize: 11, fontWeight: 700, fontFamily: FT, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", gap: 4 }}><CalendarCheck size={12} />Viewing</button>
              <button onClick={function() { setAct("agent"); }} style={{ flex: 1, padding: "10px", borderRadius: 9, border: "1.5px solid " + BD, background: "#fff", color: N, fontSize: 11, fontWeight: 700, fontFamily: FT, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", gap: 4 }}><UserCheck size={12} />Agent</button>
            </div>
          </div>
          {act && <div style={{ marginTop: 10, padding: "10px 12px", borderRadius: 9, background: GR + "08", border: "1.5px solid " + GR + "30", fontSize: 11, color: GR, fontWeight: 600, display: "flex", alignItems: "center", gap: 6 }}><CheckCircle2 size={14} />{act === "pay" ? "Redirecting..." : act === "res" ? "Reserved 7 days." : act === "view" ? "Viewing scheduled." : "Agent will call."}</div>}
        </div>
      </div>
    </div>
  );
}

export default function BookingEngine() {
  var w = typeof window !== "undefined" ? window.innerWidth : 375;
  var _w = useState(w); var width = _w[0]; var setWidth = _w[1];
  useEffect(function() { function h() { setWidth(window.innerWidth); } window.addEventListener("resize", h); return function() { window.removeEventListener("resize", h); }; }, []);
  var mob = width < 768;

  var _st = useState(1); var step = _st[0]; var setStep = _st[1];
  var _ms = useState(1); var maxS = _ms[0]; var setMaxS = _ms[1];
  var _fc = useState([]); var facs = _fc[0]; var setFacs = _fc[1];
  var _ss = useState({ tab: "type", purs: [], clim: null, climAuto: true, manSz: null, photos: [] }); var ss = _ss[0]; var setSS = _ss[1];
  var _pr = useState(null); var profile = _pr[0]; var setProfile = _pr[1];
  var _su = useState(null); var selUnit = _su[0]; var setSelUnit = _su[1];
  var _id = useState(null); var iDisc = _id[0]; var setIDisc = _id[1];
  var _ic = useState(null); var iCoup = _ic[0]; var setICoup = _ic[1];

  function go(s) { setStep(s); window.scrollTo(0, 0); }

  var css = "@import url('https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600;700;800;900&display=swap');*{box-sizing:border-box;margin:0;padding:0}::-webkit-scrollbar{height:0;width:0}.cs{display:flex;gap:6px;overflow-x:auto;padding-bottom:2px;scrollbar-width:none}.cs::-webkit-scrollbar{display:none}.fi{animation:fi .25s ease}@keyframes fi{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}.uc{transition:all .2s}@media(hover:hover){.uc:hover{box-shadow:0 8px 24px rgba(27,42,74,.1);transform:translateY(-2px)}}.uc:active{transform:scale(.98)}@keyframes spin{to{transform:rotate(360deg)}}";

  return (
    <div style={{ minHeight: "100vh", background: BG, fontFamily: FT, color: N }}>
      <style>{css}</style>
      <section style={{ position: "relative", height: mob ? 90 : 120, overflow: "hidden", background: "linear-gradient(135deg," + N + ",#2d3f63)" }}>
        <div style={{ position: "absolute", inset: 0, display: "flex", alignItems: "center", padding: mob ? "0 20px" : "0 48px" }}>
          <h1 style={{ fontSize: mob ? 20 : 26, fontWeight: 900, color: "#fff" }}>
            {step === 1 ? "Find your " : step === 2 ? "About your " : step === 3 ? "Browse " : "Book "}
            <span style={{ color: R }}>{step === 1 ? "location" : step === 2 ? "storage" : step === 3 ? "units" : "unit"}</span>
          </h1>
        </div>
      </section>
      <ProgBar step={step} maxS={maxS} go={go} mob={mob} />
      {step === 1 && <Step1 mob={mob} facs={facs} setFacs={setFacs} onNext={function() { setMaxS(function(m) { return Math.max(m, 2); }); go(2); }} />}
      {step === 2 && <Step2 mob={mob} ss={ss} setSS={setSS} onNext={function(out) {
        var p = Object.assign({}, out, { facilities: facs });
        p.locationCommitted = p.facilities.length > 0 && !p.facilities.includes("undecided");
        p.multiSite = p.facilities.filter(function(f) { return f !== "undecided"; }).length > 1;
        p.intent = !p.locationCommitted ? "price" : (p.purposeIds && p.purposeIds.length > 0) ? "product" : "location";
        setProfile(p); setMaxS(function(m) { return Math.max(m, 3); }); go(3);
      }} />}
      {step === 3 && profile && <Step3 mob={mob} profile={profile} facs={facs} onSelect={function(u, d, c) { setSelUnit(u); setIDisc(d); setICoup(c); setMaxS(function(m) { return Math.max(m, 4); }); go(4); }} />}
      {step === 4 && selUnit && <Step4 mob={mob} unit={selUnit} initDisc={iDisc} initCoupon={iCoup} />}
    </div>
  );
}
