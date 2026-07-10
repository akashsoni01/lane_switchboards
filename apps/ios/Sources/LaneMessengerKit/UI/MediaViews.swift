import SwiftUI
import UniformTypeIdentifiers
#if canImport(PhotosUI)
import PhotosUI
#endif
#if canImport(QuickLook)
import QuickLook
#endif
#if os(macOS)
import AppKit
#endif
#if os(iOS)
import UIKit
#endif

/// Attachment picker + media preview sheets used by chat threads.
struct AttachMenuView: View {
    @Bindable var model: AppModel
    #if canImport(PhotosUI)
    @State private var photoItem: PhotosPickerItem?
    #endif
    #if os(iOS)
    @State private var showCamera = false
    #endif

    var body: some View {
        NavigationStack {
            List {
                #if canImport(PhotosUI)
                Section {
                    PhotosPicker(selection: $photoItem, matching: .images) {
                        Label("Photo library", systemImage: "photo")
                    }
                }
                #endif
                #if os(iOS)
                Section {
                    Button {
                        showCamera = true
                    } label: {
                        Label("Camera", systemImage: "camera")
                    }
                }
                #endif
                Section {
                    Button {
                        model.showAttachMenu = false
                        model.showFileImporter = true
                    } label: {
                        Label("Files (PDF…)", systemImage: "doc")
                    }
                }
                Section {
                    Text("Max \(AppLimits.maxMediaBytes / (1024 * 1024)) MiB · chunked over FunXMPP")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }
            .navigationTitle("Attach")
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Close") { model.showAttachMenu = false }
                }
            }
            #if canImport(PhotosUI)
            .onChange(of: photoItem) { _, item in
                guard let item else { return }
                Task {
                    if let data = try? await item.loadTransferable(type: Data.self) {
                        let name = "photo-\(UUID().uuidString.prefix(8)).jpg"
                        await model.sendAttachment(
                            data: data,
                            fileName: name,
                            mimeType: "image/jpeg"
                        )
                        model.showAttachMenu = false
                        photoItem = nil
                    }
                }
            }
            #endif
            #if os(iOS)
            .fullScreenCover(isPresented: $showCamera) {
                CameraPicker { data in
                    showCamera = false
                    guard let data else { return }
                    Task {
                        await model.sendAttachment(
                            data: data,
                            fileName: "camera-\(UUID().uuidString.prefix(8)).jpg",
                            mimeType: "image/jpeg"
                        )
                        model.showAttachMenu = false
                    }
                }
                .ignoresSafeArea()
            }
            #endif
        }
    }
}

#if os(iOS)
struct CameraPicker: UIViewControllerRepresentable {
    var onCapture: (Data?) -> Void

    func makeUIViewController(context: Context) -> UIImagePickerController {
        let picker = UIImagePickerController()
        picker.sourceType = UIImagePickerController.isSourceTypeAvailable(.camera) ? .camera : .photoLibrary
        picker.delegate = context.coordinator
        return picker
    }

    func updateUIViewController(_ controller: UIImagePickerController, context: Context) {}

    func makeCoordinator() -> Coordinator { Coordinator(onCapture: onCapture) }

    final class Coordinator: NSObject, UIImagePickerControllerDelegate, UINavigationControllerDelegate {
        let onCapture: (Data?) -> Void
        init(onCapture: @escaping (Data?) -> Void) { self.onCapture = onCapture }

        func imagePickerControllerDidCancel(_ picker: UIImagePickerController) {
            onCapture(nil)
        }

        func imagePickerController(
            _ picker: UIImagePickerController,
            didFinishPickingMediaWithInfo info: [UIImagePickerController.InfoKey: Any]
        ) {
            let image = info[.originalImage] as? UIImage
            onCapture(image?.jpegData(compressionQuality: 0.85))
        }
    }
}
#endif

struct MediaPreviewHost: View {
    @Bindable var model: AppModel

    var body: some View {
        Group {
            if let mediaId = model.previewMediaId,
               let meta = model.mediaMeta(mediaId),
               let url = model.mediaFileURL(mediaId) {
                if meta.isImage, let data = try? Data(contentsOf: url),
                   let image = PlatformImage(data: data) {
                    ImageViewer(image: image) {
                        model.previewMediaId = nil
                    }
                } else {
                    #if os(iOS) && canImport(QuickLook)
                    QuickLookPreview(url: url) {
                        model.previewMediaId = nil
                    }
                    #else
                    VStack(spacing: 12) {
                        Text(meta.fileName)
                        Text(url.path)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                        Button("Close") { model.previewMediaId = nil }
                    }
                    .padding()
                    #endif
                }
            }
        }
    }
}

#if os(iOS) && canImport(QuickLook)
struct QuickLookPreview: UIViewControllerRepresentable {
    let url: URL
    var onDismiss: () -> Void

    func makeUIViewController(context: Context) -> UINavigationController {
        let preview = QLPreviewController()
        preview.dataSource = context.coordinator
        preview.delegate = context.coordinator
        let nav = UINavigationController(rootViewController: preview)
        preview.navigationItem.leftBarButtonItem = UIBarButtonItem(
            barButtonSystemItem: .done,
            target: context.coordinator,
            action: #selector(Coordinator.done)
        )
        return nav
    }

    func updateUIViewController(_ controller: UINavigationController, context: Context) {}

    func makeCoordinator() -> Coordinator {
        Coordinator(url: url, onDismiss: onDismiss)
    }

    final class Coordinator: NSObject, QLPreviewControllerDataSource, QLPreviewControllerDelegate {
        let url: URL
        let onDismiss: () -> Void

        init(url: URL, onDismiss: @escaping () -> Void) {
            self.url = url
            self.onDismiss = onDismiss
        }

        func numberOfPreviewItems(in controller: QLPreviewController) -> Int { 1 }
        func previewController(_ controller: QLPreviewController, previewItemAt index: Int) -> QLPreviewItem {
            url as QLPreviewItem
        }

        @objc func done() { onDismiss() }
        func previewControllerDidDismiss(_ controller: QLPreviewController) { onDismiss() }
    }
}
#endif

struct ImageViewer: View {
    let image: PlatformImage
    var onClose: () -> Void

    var body: some View {
        NavigationStack {
            #if os(iOS)
            Image(uiImage: image)
                .resizable()
                .scaledToFit()
                .background(Color.black)
                .ignoresSafeArea()
            #else
            Image(nsImage: image)
                .resizable()
                .scaledToFit()
                .background(Color.black)
                .ignoresSafeArea()
            #endif
        }
        .toolbar {
            ToolbarItem(placement: .cancellationAction) {
                Button("Close", action: onClose)
            }
        }
    }
}

#if os(iOS)
typealias PlatformImage = UIImage
#else
typealias PlatformImage = NSImage
#endif

struct MediaAttachmentView: View {
    let message: StoredMessage
    let meta: MediaBlobMeta?
    let progress: MediaTransferProgress?
    let completeURL: URL?
    var onOpen: () -> Void
    var onRetryDownload: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            if let meta, meta.complete, meta.isImage, let url = completeURL,
               let data = try? Data(contentsOf: url),
               let image = PlatformImage(data: data) {
                Button(action: onOpen) {
                    #if os(iOS)
                    Image(uiImage: image)
                        .resizable()
                        .scaledToFill()
                        .frame(maxWidth: 220, maxHeight: 220)
                        .clipped()
                        .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))
                    #else
                    Image(nsImage: image)
                        .resizable()
                        .scaledToFill()
                        .frame(maxWidth: 220, maxHeight: 220)
                        .clipped()
                        .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))
                    #endif
                }
                .buttonStyle(.plain)
            } else {
                Button(action: {
                    if completeURL != nil { onOpen() } else { onRetryDownload() }
                }) {
                    HStack {
                        Image(systemName: iconName)
                        VStack(alignment: .leading, spacing: 2) {
                            Text(meta?.fileName ?? message.mediaId)
                                .font(.subheadline.weight(.medium))
                                .lineLimit(1)
                            Text(statusText)
                                .font(.caption2)
                                .foregroundStyle(.secondary)
                        }
                    }
                    .padding(10)
                    .background(Color.secondary.opacity(0.12), in: RoundedRectangle(cornerRadius: 12))
                }
                .buttonStyle(.plain)
            }

            if let progress, progress.status == .transferring || progress.status == .queued {
                ProgressView(value: progress.fraction)
                    .frame(maxWidth: 220)
            }
            if let progress, progress.status == .failed {
                Text(progress.error ?? "Transfer failed")
                    .font(.caption2)
                    .foregroundStyle(.red)
            }
        }
    }

    private var iconName: String {
        if meta?.isPDF == true { return "doc.richtext" }
        if meta?.isImage == true { return "photo" }
        return "paperclip"
    }

    private var statusText: String {
        if meta?.complete == true { return "Tap to open" }
        if progress?.status == .failed { return "Tap to retry download" }
        if progress?.status == .transferring { return "Downloading…" }
        return "Tap to download"
    }
}
